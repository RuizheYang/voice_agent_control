# 线索相关 Webhook 接口文档 (Clue Webhook API)

本文件定义了 `clue_webhook.py` 下提供的所有用于线索管理和外呼策略相关的回调接口。    
**测试环境url: 10.10.202.186:8000**

## 1. 统一响应格式与异常处理约定
为了方便所有上游回调系统统一处理，本 Webhook 下的所有接口均采用同一套返回值格式结构。
无论内部流转成功还是业务被阻断，前端收到的 HTTP 状态码均为 `HTTP 200 OK`，调用方需根据 JSON 包体内的 `code` 字段判断处理结果及是否需要发起重试。

### 统一响应结构 (Response Body)
| 序号 | 字段名 | 说明 | 类型 | 是否必填 | 备注 |
| :---: | :--- | :--- | :--- | :---: | :--- |
| 1 | **code** | 响应码值 | Integer | 是 | 核心状态码（详见下表） |
| 2 | **msg** | 响应描述信息 | String | 是 | 附加的具体提示信息或错误原因 |

### `code` 响应码详解表
| code 值 | 含义 | 说明及其排查建议 | 是否需要重试 |
| :---: | :--- | :--- | :---: |
| **0** | **处理成功** | 接口通知接收并且业务流转成功 | **否** |
| **1** | **状态冲突 / 已废弃** (`StateTransitionError`) | 触发了非法的状态流转，例如已经结束的线索又收到了超时的无效回调等。系统已视为逻辑错误强行拦截。 | **否** (勿重试，防止由于旧消息积压引起的重试风暴) |
| **2** | **并发/锁超时** (`LockTimeoutError`) | 系统处理该线索的并发数过高导致无法获取排他锁，当前任务被卡住。 | **是** (建议稍后延迟几秒再发起重试) |
| **3** | **系统内部异常** | 发生了未捕获的运行时内部异常。 | **视系统要求而定（@杨芮哲）** |

**响应示例:**
```json
{
    "code": 0,
    "msg": "success"
}
```

---

## 2. 接口列表

所有接口的基础路径 (Prefix) 为：`/clue`

### 2.1 新线索入列
在业务层产生新线索时，将线索推入后台队列，进行统一管控并缓慢发起外呼。  
**调用方式**：业务层调用加微 Agent

- **路径**: `/clue/new/queue`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **workFormId** | String | 是 | 任务唯一ID，由业务系统提供 |
| **clueId** | String | 是 | 线索ID |
| **clueSource** | String | 是 | 线索来源 |
| **clueSourceType** | String | 是 | 线索来源类型，用于生成线索截图 |
| **clueType** | String | 是 | 线索类型，用于生成线索截图 |
| **clueTime** | String | 是 | 线索流入时间，用于生成线索截图 |
| **orderTime** | String | 是 | 下单时间 |
| **isShare** | String | 否 | 是否共享，用于生成线索截图 |
| **thirdOrderId** | String | 是 | 三方订单号，用于生成线索截图 |
| **isPreCheck** | String | 否 | 是否预审，用于生成线索截图 |
| **preCheckRes** | String | 否 | 预审结果，用于生成线索截图 |
| **customerName** | String | 是 | 客户姓名 |
| **customerPhoneNum** | String | 是 | 客户手机号 |
| **customerPhoneCity** | String | 否 | 手机号归属地，用于生成线索截图 |
| **customerSex** | String | 否 | 客户性别 |
| **customerCity** | String | 是 | 客户所在省市 |
| **callBackUrl** | String | 是 | 回调接口 |
| **accessReceiveTime** | String | 是 | 接入系统接收时间，格式须严格为 `yyyy-MM-dd HH:mm:ss` |
| **slots** | Object | 是 | 实验槽位对象 (含 `slotCallTts`, `slotCallPolicy`)，线上暂不支持 |
| **agentDomainAccount** | String | 是 | 坐席域账号 |
| **agentAccount** | String | 是 | 坐席姓名 |
| **agentMobile** | String | 否 | 坐席手机号 |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.2 新线索直接处理
业务层推入新线索后，立刻触发处理逻辑（发起首呼或进行相应的排期），不经过缓存排队。  
**调用方式**：业务层调用加微 Agent

- **路径**: `/clue/new`
- **方法**: `POST`

**请求参数**：与 `2.1 新线索入列` 完全一致。

**响应参数 (Response Body)**
遵循第一节的《统一响应结构》。

---

### 2.3 外呼开始通知
接收外呼(Voice Agent)系统推送的“通话已开始”的通知信号。  
**调用方式**：Voice Agent 在外呼开始时调用加微 Agent

- **路径**: `/clue/call-start/notify`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **clueId** | String | 是 | 线索ID，必须是接入系统内已存在的线索 |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.4 外呼结束回调
外呼结束(Voice Agent)时接收回调结果。包含具体的通话录音、时间、意向探查结果等详情。底层会根据外呼情况触发内部线索策略引擎，从而判断下一步（加粉、挂断、重呼等）。  
**调用方式**：Voice Agent 通话结束后调用加微 Agent

- **路径**: `/clue/call-end/notify`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **msgType** | String | 是 | 消息类型 (默认为 `pre_agent`) |
| **data** | Object | 是 | 回调消息核心包体 (包含外呼报告及历史详情，结构见下表) |

**`data` 对象详解**:
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **clueId** | String | 是 | 线索 ID |
| **callbackType** | String | 是 | 回调类型 (通常为 `外呼回调`) |
| **outboundReport** | Object | 是 | 探需外呼模型报告 (含业务校验状态 `userQualified`, `conversationalSummary`, 是否接通 `callAnswered`, 微信请求 `wechatRequested`, 是否生气 `isAngry`, 下次呼叫 `nextCallTime`, AI一/二级识别码 `oneLevelCode`, `twoLevelCode` 等) |
| **currentCall** | Object | 是 | 本次通话物理信息与明细 (含 `callId`, 振铃时间 `ringingStartTime`, 接通时间 `talkStartTime`, 通话时长 `aiCallSeconds`, 录音文件地址 `recordFile` 等全量呼叫信息) |
| **historicalCalls**| Array | 否 | 历史通话记录列表 |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.5 计划任务回调 (排队/次呼)
当定时排期外呼（例如次呼、重呼）到达触发时间点时，或者等待一定时间的微信通知响应到期时，由此入口唤醒线索进行下一步业务处理。  
**调用方式**：接入系统调用加微 Agent

- **路径**: `/clue/schedule/notify`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **clueId** | String | 是 | 线索ID，接入系统内存在的线索 |
| **slots** | Object | 是 | 实验槽位对象 (含 `slotCallTts`, `slotCallPolicy` 等) |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.6 通道通知回调 (企微意向动作回调)
从接入系统接收企微等外部通道动作执行后的回调结果（例如：搜索手机号是否找到用户，好友邀请是否成功发送，是否遇到资源超限阻拦等底层信号）。  
**调用方式**：通道调用加微 Agent

- **路径**: `/clue/channel/notify`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **msgType** | String | 是 | 消息类型 (如 `pre_agent`) |
| **data** | Object | 是 | 通道动作结果详情数据体 |

**`data` 对象详解**:
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **type** | String | 是 | 指令类型 (如 `notice_addF`) |
| **resultCode** | String | 是 | 微信添加进度反馈码: `1`-未找到用户, `3`-添加成功, `4`-添加失败, `434`-无可用资源 |
| **resultMessage**| String | 否 | 添加失败时，此处为具体的失败原因说明 |
| **pipeNickName** | String | 否 | 分配的通道昵称（仅在添加成功时才有值）|
| **custId** | String | 是 | 客户的外部业务ID |
| **customerAdditionType**| String | 否 | 客户添加途径方式: `1`-搜手机号添加, `2`-扫码添加... |
| **slots** | Object | 是 | 实验槽位 |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.7 取消线索请求
从业务层接收取消指定线索的通知，后端将强行拦截/中止对应线索后续的拨打计划或企微跟进动作。  
**调用方式**：业务层调用加微 Agent

- **路径**: `/clue/cancel`
- **方法**: `POST`

#### 请求参数 (Request Body)
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **clueId** | String | 是 | 需取消/废弃的线索ID |

#### 响应参数 (Response Body)
遵循第一节的《统一响应结构》。

---

### 2.8 线索上下文查询
用于通过线索 ID 查询该线索的上下文信息实体数据。

**调用方式**：外部业务系统调用以获取线索的全量流转与状态上下文

- **完整路径**: `/database/query/context` (注意：此接口属于 database 模块，主路径不是 `/clue`)
- **方法**: `POST`

#### 请求参数 (Request Body)
由于启用了 body 的 embed 模式，需包裹成 JSON 对象传入。
| 字段名 | 类型 | 必填 | 说明 |
| :--- | :--- | :---: | :--- |
| **clueId** | String | 是 | 查询目标线索的ID |

**请求体示例**:
```json
{
    "clueId": "xxxx"
}
```

#### 响应参数 (Response Body)
直接返回该线索对应的完整上下文信息 (Content)。注：此接口直接透传了底层实体数据包，**未**采用第一节的统一 `code/msg` 结构。


#### 响应包体 (Response Body) 字段详解

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| **userQualified** | Boolean | 用户资质是否合格或业务是否符合要求，有意向 & 有车 & 没有抵押 & 绿本在手 |
| **conversationSummary** | String | 本次通话/跟进的对话小结及摘要信息 |
| **checkDetails** | Object | 具体的业务要素核对详情（探需结果） |
| ├── name_check | Boolean/Null | 客户姓名是否核对一致 |
| ├── need_financial_service | Boolean/Null | 客户是否需要金融服务 |
| ├── is_car_owner | Boolean/Null | 是否为车主 |
| ├── under_repayment | Boolean/Null | 是否仍在还款中 |
| └── has_driving_license | Boolean/Null | 是否绿本在手 |
| **isPhoneNumber** | Boolean/Null | (微信)是否与手机号一致 |
| **wechatAccount** | String/Null | 客户微信号 |
| **isUserOperable** | Boolean | 客户是否会操作微信 |
| **nextCallTime** | String/Null | 约定的下次呼叫时间 |
| **isAngry** | Boolean | 客户是否表现出生气、反感或明显的不耐烦情绪 |
| **wechatRequested** | Boolean | 是否在通话中向客户请求/提出过添加微信 |
| **wechatAccepted** | Boolean | 客户是否成功添加微信 |
| **memory** | Array | AI 与客户的对话历史/聊天记录列表 |
| ├── role | String | 发言角色标识 (如 `assistant` 为AI客服, `user` 为客户本人) |
| └── content | String | 具体的通话/发言内容文本 |
| **oneLevelCode** | String | AI 判别的一级意向意图/接通状态码 (如 `"300"`) |
| **twoLevelCode** | String | AI 判别的二级细分意向/挂断原因码 (如 `"312"`) |
| **callAnswered** | Boolean | 本次外呼是否成功接通 |
| **recordFile** | String | 本次通话完整的录音文件下载链接 (URL) |
| **hasAnsweredCalls** | Boolean | 该线索过往（含历史通话）是否曾有过接通记录 |
| **state** | String | 线索当前生命周期的流转状态 (如 `rejected` 代表被拒绝/废弃，通过为 `accepted`) |

注：bool值为true，表示满足条件， false表示不满足这个字段的条件，null表示该通对话未提及这个话题