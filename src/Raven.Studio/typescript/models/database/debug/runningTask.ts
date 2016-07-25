class runningTask {
    id: number;
    status: operationStateDto;
    taskType: string;
    startTime: string;
    description: string;
    completed: boolean;
    faulted: boolean;
    canceled: boolean;
    executionStatus: string;
    timeStampText: KnockoutComputed<string>;
    exceptionText: string;
    killable: boolean;
    fullStatus: string;

    constructor(dto: runningTaskDto) {
        this.id = dto.Id;
        this.status = dto.Status;
        this.taskType = dto.TaskType;
        this.description = dto.Description;
        this.completed = dto.Completed;
        this.faulted = dto.Faulted;
        this.canceled = dto.Canceled;

        this.fullStatus = runningTask.createFullStatus(dto);

        this.startTime = dto.StartTime; // used for sorting
        this.timeStampText = this.createHumanReadableTime(dto.StartTime);
        this.exceptionText = dto.Exception ? JSON.stringify(dto.Exception, null, 2) : null;
        this.killable = dto.Killable;
    }

    private static createFullStatus(dto: runningTaskDto) {
        var taskState = "Running";
        if (dto.Canceled) {
            taskState = "Canceled";
        } else if (dto.Faulted) {
            taskState = "Faulted";
        } else if (dto.Completed) {
            taskState = "Completed";
        }

        var status: string = null;
        if (dto.Status) {
            status = dto.Faulted || dto.Canceled ? dto.Status.Error : dto.Status.Progress;
        }

        return status ? taskState + ": " + status : taskState;
    }

    createHumanReadableTime(time: string): KnockoutComputed<string> {
        var now = moment();
        if (time) {
            return ko.computed(() => {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(now);
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            });
        }

        return ko.computed(() => time);
    }
}

export = runningTask;