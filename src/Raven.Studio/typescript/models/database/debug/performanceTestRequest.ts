/// <reference path="../../../../typings/tsd.d.ts"/>

class performanceTestRequest {

    path = ko.observable<string>();
    fileSize = ko.observable<number>();

    testType = ko.observable<string>("generic");

    // generic type specific options:
    operationType = ko.observable<string>();
    bufferingType = ko.observable<string>();
    sequential = ko.observable<boolean>();
    threadCount = ko.observable<number>();
    timeToRunInSeconds = ko.observable<number>();
    //random seed - we don't expose on UI
    chunkSize = ko.observable<number>();

    // batch type specific options:
    numberOfDocuments = ko.observable<number>();
    sizeOfDocuments = ko.observable<number>();
    numberOfDocumentsInBatch = ko.observable<number>();
    waitBetweenBatches = ko.observable<number>();

    constructor(dto: performanceTestRequestDto) {
        this.path(dto.Path);
        this.fileSize(dto.FileSize);
        this.testType(dto.TestType);

        this.operationType(dto.OperationType);
        this.bufferingType(dto.BufferingType);
        this.sequential(dto.Sequential);
        this.threadCount(dto.ThreadCount);
        this.timeToRunInSeconds(dto.TimeToRunInSeconds);
        this.chunkSize(dto.ChunkSize);

        this.numberOfDocuments(dto.NumberOfDocuments);
        this.sizeOfDocuments(dto.SizeOfDocuments);
        this.numberOfDocumentsInBatch(dto.NumberOfDocumentsInBatch);
        this.waitBetweenBatches(dto.WaitBetweenBatches);
    }

    static empty(): performanceTestRequest {
        return new performanceTestRequest({
            TestType: "generic",
            Path: "c:\\temp\\",
            FileSize: 1024 * 1024 * 1024,
            OperationType: "Write",
            BufferingType: "None",
            Sequential: true,
            ThreadCount: 4,
            TimeToRunInSeconds: 30,
            ChunkSize: 4 * 1024,
            NumberOfDocuments: 300000,
            SizeOfDocuments: 8 * 1024,
            NumberOfDocumentsInBatch: 200,
            WaitBetweenBatches: 0
        });
    }

    toDto(): performanceTestRequestDto {
        if (this.testType() == "generic") {
            return {
                TestType: this.testType(),
                Path: this.path(),
                FileSize: this.fileSize(),
                OperationType: this.operationType(),
                BufferingType: this.bufferingType(),
                Sequential: this.sequential(),
                ThreadCount: this.threadCount(),
                TimeToRunInSeconds: this.timeToRunInSeconds(),
                ChunkSize: this.chunkSize()
            };
        } else {
            return {
                TestType: this.testType(),
                Path: this.path(),
                FileSize: this.fileSize(),
                NumberOfDocuments: this.numberOfDocuments(),
                SizeOfDocuments: this.sizeOfDocuments(),
                NumberOfDocumentsInBatch: this.numberOfDocumentsInBatch(),
                WaitBetweenBatches: this.waitBetweenBatches()
            };
        }
    }
}

export = performanceTestRequest;
