class performanceTestRequest {

    path = ko.observable<string>();
    fileSize = ko.observable<number>();
    operationType = ko.observable<string>();
    bufferingType = ko.observable<string>();
    sequential = ko.observable<boolean>();
    threadCount = ko.observable<number>();
    timeToRunInSeconds = ko.observable<number>();
    //random seed - we don't expose on UI
    chunkSize = ko.observable<number>();

    constructor(dto: performanceTestRequestDto) {
        this.path(dto.Path);
        this.fileSize(dto.FileSize);
        this.operationType(dto.OperationType);
        this.bufferingType(dto.BufferingType);
        this.sequential(dto.Sequential);
        this.threadCount(dto.ThreadCount);
        this.timeToRunInSeconds(dto.TimeToRunInSeconds);
        this.chunkSize(dto.ChunkSize);
    }

    static empty(): performanceTestRequest {
        return new performanceTestRequest({
            Path: "c:\\temp\\",
            FileSize: 1024 * 1024 * 1024,
            OperationType: "Write",
            BufferingType: "None",
            Sequential: true,
            ThreadCount: 4,
            TimeToRunInSeconds: 30,
            ChunkSize: 4 * 1024
        });
    }

    toDto(): performanceTestRequestDto {
        return {
            Path: this.path(),
            FileSize: this.fileSize(),
            OperationType: this.operationType(),
            BufferingType: this.bufferingType(),
            Sequential: this.sequential(),
            ThreadCount: this.threadCount(),
            TimeToRunInSeconds: this.timeToRunInSeconds(),
            ChunkSize: this.chunkSize()
        };
    }
}

export = performanceTestRequest;