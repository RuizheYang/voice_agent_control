from enum import Enum

class ClueState(str, Enum):
    
    CALLING = "calling" # 呼叫中，已发起呼叫但未结束
    SCHEDULED = "scheduled" # 已安排定时任务等待呼叫，尚未发起呼叫
    PENDING = "pending" # 已发送微信添加请求，等待通过
    REJECTED = "rejected" # 已拒绝，微信添加失败或用户拒绝添加
    ACCEPTED = "accepted" # 已接受，微信添加成功
    CREATED = "created" # 线索已创建，尚未安排定时任务或发起呼叫
    WAIT_FOR_CALLING = "wait_for_calling" # 已发起呼叫任务，在话务平台中等待呼出
    ERROR = "error" # 错误状态
    SUMMARY = "summary" # Summary推断中
    AWAITING_APPROVAL = "awaiting_approval" # Deferred Tool Call 挂起等待人工审批
    
    
VALID_TRANSITIONS = {
    # 初始状态和中间状态
    
    ## 线索创建后，可以直接安排定时任务等待呼叫，或者直接发起呼叫，或者直接取消/放弃
    ClueState.CREATED:   [ClueState.SCHEDULED, ClueState.CALLING, ClueState.WAIT_FOR_CALLING, ClueState.REJECTED],
    
    ## 已安排呼叫任务，但是并没有真正的拨打电话，在话务系统中排队
    ClueState.WAIT_FOR_CALLING: [ClueState.CALLING, ClueState.REJECTED, ClueState.SCHEDULED, ClueState.SUMMARY],
    
    ## 通话状态，结束后可能等待加微成功，可能直接放弃线索，或者直接安排下一次呼叫，或者进入 SUMMARY 推断
    ClueState.CALLING:   [ClueState.PENDING, ClueState.REJECTED, 
                          ClueState.SCHEDULED, ClueState.ACCEPTED, ClueState.ERROR, ClueState.SUMMARY, ClueState.AWAITING_APPROVAL],
    
    ## 定时任务状态，到了时间点后准备发起呼叫(CALLING)，或者直接放弃线索/结案
    ClueState.SCHEDULED: [ClueState.CALLING, ClueState.WAIT_FOR_CALLING, ClueState.REJECTED, ClueState.ACCEPTED, ClueState.AWAITING_APPROVAL],
    
    ## 等待加微状态，等待通过后可以直接放弃线索，或者添加成功后进入接受状态，或者重试排期
    ClueState.PENDING:   [ClueState.ACCEPTED, ClueState.REJECTED, ClueState.SCHEDULED, ClueState.AWAITING_APPROVAL], 
    
    # 终态：无法再流转到别的状态！
    
    ## 已经添加微信成功或外呼转化达成，线索完成，无法再流转到其他状态了
    ClueState.ACCEPTED: [],
    
    ## 已经拒绝，线索完成，无法再流转到其他状态了
    ClueState.REJECTED: [],
    
    ## Summary推断中，成功后流转到PENDING/ACCEPTED等，失败流转到REJECTED，需追呼流转到SCHEDULED，高危工具需人工审批流转到AWAITING_APPROVAL
    ClueState.SUMMARY: [ClueState.PENDING, ClueState.REJECTED, ClueState.ACCEPTED, ClueState.SCHEDULED, ClueState.AWAITING_APPROVAL],

    ## 人工审批挂起态：批准/驳回后唤醒恢复至 SCHEDULED/PENDING/ACCEPTED/REJECTED
    ClueState.AWAITING_APPROVAL: [ClueState.ACCEPTED, ClueState.REJECTED, ClueState.SCHEDULED, ClueState.PENDING],
}
