class StateTransitionError(Exception):
    """Exception raised when an invalid state transition is attempted."""
    def __init__(self, message: str, clue_id: str = None):
        super().__init__(message)
        self.clue_id = clue_id

class LockTimeoutError(Exception):
    """Exception raised when failing to acquire a distributed lock in time."""
    def __init__(self, message: str, clue_id: str = None):
        super().__init__(message)
        self.clue_id = clue_id


class WorkflowAgentPairingError(ValueError):
    """Exception raised when a workflow has no paired VoiceAgent or OutboundStrategy."""
    def __init__(self, message: str = "此workflow_id业务线没有对应的agent可以配套", workflow_id: str = None):
        super().__init__(message)
        self.workflow_id = workflow_id

