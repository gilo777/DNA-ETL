
class LoadException(Exception):
    """Raised when data loading/output generation fails for a specific participant"""
    def __init__(self, participant_id):
        self.participant_id = participant_id
        super().__init__(f"Failed to load data for participant {participant_id}")