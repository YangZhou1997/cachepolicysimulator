class Job:
    def __init__(self, user_id):
        self.user_id = user_id
        self.stages = list()

    def set_stages(self,stages):
        self.stages = stages

    def search_stage_by_id(self,stage_id):
        for stage in self.stages:
            if stage.id == stage_id:
                return stage

        return False
