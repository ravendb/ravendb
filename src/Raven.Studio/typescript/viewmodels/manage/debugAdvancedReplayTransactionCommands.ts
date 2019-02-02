import viewModelBase = require("viewmodels/viewModelBase");
import databasesManager = require("common/shell/databasesManager");
import replayTransactionsCommand = require("commands/database/debug/replayTransactionsCommand");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import getNextOperationId = require("commands/database/studio/getNextOperationId");
import notificationCenter = require("common/notifications/notificationCenter");

class debugAdvancedReplayTransactionCommands extends viewModelBase {

    private static readonly filePickerTag = "#transactionCommandsFilePicker";

    databaseNames = ko.observableArray<string>([]);
    databaseName = ko.observable<string>();

    importedFileName = ko.observable<string>(); 

    isUploading = ko.observable<boolean>(false);
    uploadStatus = ko.observable<number>();

    validationGroup: KnockoutValidationGroup;
    
    // marker for parent router
    static preventParentGrow = true;

    constructor() {
        super();

        this.initValidation();
    }

    private initValidation() {
        this.importedFileName.extend({
            required: true
        });
        
        this.databaseName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            databaseName: this.databaseName,
            importedFileName: this.importedFileName
        });
    }

    activate(args: any) {
        super.activate(args);

        this.databaseNames(databasesManager.default.databases().map(x => x.name));
    }

    startReplay() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }
        
        const db = databasesManager.default.getDatabaseByName(this.databaseName());
        const fileInput = document.querySelector(debugAdvancedReplayTransactionCommands.filePickerTag) as HTMLInputElement;

        this.isUploading(true);

        this.getNextOperationId(db)
            .done((operationId: number) => {
                const dbActivationTask = $.Deferred<void>();
                
                if (this.activeDatabase() === db) {
                    dbActivationTask.resolve();
                } else {
                    databasesManager.default.activate(db, { waitForNotificationCenterWebSocket: true })
                        .done(() => dbActivationTask.resolve());
                }
                
                dbActivationTask.done(() => {
                    notificationCenter.instance.openDetailsForOperationById(db, operationId);

                    new replayTransactionsCommand(db, operationId, fileInput.files[0], this.isUploading, this.uploadStatus)
                        .execute()
                        .always(() => {
                            this.isUploading(false);
                        });
                });
            })
    }

    private getNextOperationId(db: database): JQueryPromise<number> {
        return new getNextOperationId(db).execute()
            .fail((qXHR, textStatus, errorThrown) => {
                messagePublisher.reportError("Could not get next task id.", errorThrown);
                this.isUploading(false);
            });
    }

    fileSelected(fileName: string) {
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.importedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
    }
    
}

export = debugAdvancedReplayTransactionCommands;
