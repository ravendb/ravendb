import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import getStatusDebugConfigCommand = require("commands/getStatusDebugConfigCommand");
import appUrl = require("common/appUrl");
import monitorRestoreCommand = require("commands/monitorRestoreCommand");
import performanceTestRequest = require("models/performanceTestRequest");
import performanceTestResultWrapped = require("models/performanceTestResultWrapped");
import ioTestCommand = require("commands/ioTestCommand");
import killRunningTaskCommand = require("commands/killRunningTaskCommand");

class ioTest extends viewModelBase {

    isBusy = ko.observable<boolean>(false);
    ioTestRequest: performanceTestRequest = performanceTestRequest.empty();
    testResult = ko.observable<performanceTestResultWrapped>();

    lastCommand: ioTestCommand = null;

    chunkSizeCustomValidityError: KnockoutComputed<string>;

    fileSizeMb = ko.computed({
        read: () => this.ioTestRequest.fileSize() / 1024 / 1024,
        write: (value:number) => this.ioTestRequest.fileSize(value * 1024 * 1024)
    });

    chunkSizeKb = ko.computed({
        read: () => this.ioTestRequest.chunkSize() / 1024,
        write: (value:number) => this.ioTestRequest.chunkSize(value * 1024)
    });

    constructor() {
        super();

        this.ioTestRequest.sequential(false);

        this.chunkSizeCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            if (isNaN(this.chunkSizeKb()) || this.chunkSizeKb() % 4 != 0) {
                errorMessage = "Chunk size must be multiple of 4";
            }
            return errorMessage;
        });
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new getStatusDebugConfigCommand(appUrl.getSystemDatabase())
            .execute()
            .done((results: any) =>
                this.ioTestRequest.threadCount(results.MaxNumberOfParallelProcessingTasks))
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    onIoTestCompleted(result: diskPerformanceResultWrappedDto) {
        this.testResult(new performanceTestResultWrapped(result)); 

        //TODO: plot charts
    }

    killTask() {
        if (this.lastCommand !== null) {
            this.lastCommand.operationIdTask.done((operationId) => {
                new killRunningTaskCommand(appUrl.getSystemDatabase(), operationId).execute();
            });
        }
    }

    startPerformanceTest() {
        this.isBusy(true);
        var self = this;

        var diskTestParams = this.ioTestRequest.toDto();

        require(["commands/ioTestCommand"], ioTestCommand => {
            this.lastCommand = new ioTestCommand(appUrl.getSystemDatabase(), diskTestParams);
            this.lastCommand
                .execute()
                .done(() => {
                    new getDocumentWithMetadataCommand("Raven/Disk/Performance", appUrl.getSystemDatabase())
                        .execute()
                        .done((result: diskPerformanceResultWrappedDto) => {
                            this.onIoTestCompleted(result);
                        });
                })
                .always(() => this.isBusy(false));
        });
    }
}

export = ioTest;  