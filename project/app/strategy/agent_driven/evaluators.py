from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Type
from pydantic import BaseModel, Field, create_model

from app.strategy.agent_driven.models import (
    IntentAnalysisOutput,
    ObjectionAnalysisOutput,
    AppointmentAnalysisOutput,
    SentimentAnalysisOutput,
)
from app.strategy.agent_driven.prompts import (
    INTENT_ANALYSIS_PROMPT,
    OBJECTION_ANALYSIS_PROMPT,
    APPOINTMENT_ANALYSIS_PROMPT,
    SENTIMENT_ANALYSIS_PROMPT,
    DECLARATIVE_EVALUATOR_PROMPT,
)
from app.strategy.agent_driven.tools import format_conversation_for_prompt
from app.config import config

logger = logging.getLogger(__name__)

_FIELD_TYPE_MAP: Dict[str, Any] = {
    "str": str,
    "string": str,
    "int": int,
    "integer": int,
    "float": float,
    "number": float,
    "bool": bool,
    "boolean": bool,
    "list": List[str],
}


def build_declarative_output_model(name: str, output_fields: Dict[str, str]) -> Type[BaseModel]:
    """根据声明式字段配置，利用 pydantic.create_model 动态生成强类型评估模型"""
    fields_spec: Dict[str, Any] = {}
    for fname, ftype_str in (output_fields or {}).items():
        py_type = _FIELD_TYPE_MAP.get(str(ftype_str).strip().lower(), str)
        default_val: Any = False if py_type is bool else (0 if py_type in (int, float) else ([] if py_type == List[str] else ""))
        fields_spec[fname] = (py_type, Field(default=default_val, description=f"业务提取字段: {fname}"))
    fields_spec.setdefault("summary", (str, Field(default="", description="维度分析摘要")))
    safe_cls_name = f"DynEval_{''.join(c if c.isalnum() else '_' for c in name)}"
    return create_model(safe_cls_name, **fields_spec)


@dataclass
class RegisteredEvaluator:
    name: str
    prompt_template: str
    output_type: Type[BaseModel]
    is_declarative: bool = False


class EvaluatorRegistry:
    """可扩展子分析器注册中心：内置 4 路基础分析器 + 动态业务插件"""

    def __init__(self) -> None:
        self._registry: Dict[str, RegisteredEvaluator] = {
            "intent": RegisteredEvaluator("intent", INTENT_ANALYSIS_PROMPT, IntentAnalysisOutput),
            "objection": RegisteredEvaluator("objection", OBJECTION_ANALYSIS_PROMPT, ObjectionAnalysisOutput),
            "appointment": RegisteredEvaluator("appointment", APPOINTMENT_ANALYSIS_PROMPT, AppointmentAnalysisOutput),
            "sentiment": RegisteredEvaluator("sentiment", SENTIMENT_ANALYSIS_PROMPT, SentimentAnalysisOutput),
        }

    def register(self, evaluator: RegisteredEvaluator) -> None:
        self._registry[evaluator.name] = evaluator

    def get(self, name: str) -> Optional[RegisteredEvaluator]:
        return self._registry.get(name)

    def resolve_for_domain(
        self,
        enabled_types: Optional[List[str]] = None,
        custom_evaluators: Optional[List[Any]] = None,
    ) -> List[RegisteredEvaluator]:
        """按业务卡带动态解析需并发运行的内置+声明式子评估器"""
        resolved: List[RegisteredEvaluator] = []
        base_names = enabled_types if enabled_types is not None else ["intent", "objection", "appointment", "sentiment"]
        for name in base_names:
            ev = self._registry.get(name)
            if ev:
                resolved.append(ev)

        for spec in (custom_evaluators or []):
            if isinstance(spec, dict):
                ev_name = spec.get("name", "custom")
                ev_prompt = spec.get("prompt", "")
                out_fields = spec.get("output_fields", {})
            else:
                ev_name = getattr(spec, "name", "custom")
                ev_prompt = getattr(spec, "prompt", "")
                out_fields = getattr(spec, "output_fields", {})
            if not ev_name or not ev_prompt:
                continue
            dyn_model = build_declarative_output_model(ev_name, out_fields)
            prompt_text = DECLARATIVE_EVALUATOR_PROMPT.format(
                evaluator_name=ev_name,
                task_prompt=ev_prompt,
                conversation_text="{conversation_text}",
            )
            resolved.append(
                RegisteredEvaluator(
                    name=ev_name,
                    prompt_template=prompt_text,
                    output_type=dyn_model,
                    is_declarative=True,
                )
            )
        return resolved


evaluator_registry = EvaluatorRegistry()


class SubPromptEvaluatorsPipeline:
    """
    垂直业务场景 Prompt 并发执行流水线 (Map 阶段)
    支持内置基础 4 路与业务卡带声明式子评估器一并并发运行，互不阻断。
    """

    def __init__(self, model_runner: Optional[Any] = None, registry: Optional[EvaluatorRegistry] = None):
        self._model_runner = model_runner
        self._registry = registry or evaluator_registry

    def _get_model(self):
        """获取共享或新初始化的 Pydantic-AI 模型"""
        try:
            from pydantic_ai.models.openai import OpenAIChatModel
            from pydantic_ai.providers.openai import OpenAIProvider

            llm_cfg = config.llm_factory_config
            if not llm_cfg or not llm_cfg.base_url:
                return None
            provider = OpenAIProvider(base_url=llm_cfg.base_url, api_key=llm_cfg.api_key or "sk-dummy")
            return OpenAIChatModel(llm_cfg.model_name or "default-model", provider=provider)
        except Exception as e:
            logger.debug(f"[SubPromptEvaluators] 初始化模型失败或未安装完整环境: {e}")
            return None

    async def _run_single_evaluator(
        self,
        prompt_text: str,
        output_type: Type[BaseModel],
        evaluator_name: str,
    ) -> BaseModel:
        """执行单个垂直维度的 Agent 分析"""
        try:
            from pydantic_ai import Agent

            model = self._get_model()
            if not model:
                logger.debug(f"[SubPromptEvaluators] 无可用模型，使用默认值: {evaluator_name}")
                return output_type()

            agent = Agent(model=model, output_type=output_type, defer_model_check=True)
            result = await asyncio.wait_for(agent.run(prompt_text), timeout=3.0)
            return result.output
        except Exception as err:
            logger.warning(f"[SubPromptEvaluators] {evaluator_name} 执行异常 (已使用兜底结论): {err}")
            return output_type()

    async def evaluate_all(
        self,
        messages: List[Dict[str, Any]] | List[Any],
        reference_time: str,
        enabled_types: Optional[List[str]] = None,
        custom_evaluators: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """
        并发执行所有启用的基础+业务声明式场景分析器
        """
        conversation_text = format_conversation_for_prompt(messages)
        active_evaluators = self._registry.resolve_for_domain(
            enabled_types=enabled_types,
            custom_evaluators=custom_evaluators,
        )

        tasks = []
        task_names: List[str] = []
        type_map: Dict[str, Type[BaseModel]] = {}

        for ev in active_evaluators:
            formatted_prompt = ev.prompt_template.format(
                conversation_text=conversation_text,
                reference_time=reference_time,
            )
            tasks.append(
                self._run_single_evaluator(
                    formatted_prompt, ev.output_type, ev.name
                )
            )
            task_names.append(ev.name)
            type_map[ev.name] = ev.output_type

        if not tasks:
            return {}

        logger.info(f"[SubPromptEvaluators] 【开始】并发执行业务场景分析器: {task_names}")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        final_outputs: Dict[str, Any] = {}
        for name, res in zip(task_names, results):
            if isinstance(res, Exception):
                logger.error(f"[SubPromptEvaluators] 分析器 {name} 抛出异常: {res}")
                final_outputs[name] = type_map[name]()
            else:
                final_outputs[name] = res

        logger.info(f"[SubPromptEvaluators] 【完成】并发业务分析完成: {list(final_outputs.keys())}")
        return final_outputs
