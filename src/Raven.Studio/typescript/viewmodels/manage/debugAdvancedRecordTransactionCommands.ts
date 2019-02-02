import viewModelBase = require("viewmodels/viewModelBase");
import databasesManager = require("common/shell/databasesManager");
import recordTransactionsCommand = require("commands/database/debug/recordTransactionsCommand");
import notificationCenter = require("common/notifications/notificationCenter");
import operation = require("common/notifications/models/operation");
import stopRecordingTransactionsCommand = require("commands/database/debug/stopRecordingTransactionsCommand");

class runningRecording {
    databaseName: string;
    operationId: number;
    filePath: string;
    highlight = ko.observable<boolean>(false);
    inProgress = ko.observable<boolean>();
    
    static for(details: Raven.Server.Documents.Handlers.TransactionsRecordingHandler.RecordingDetails, 
               operationId: number, 
               inProgress: KnockoutComputed<boolean>) {
        const recording = new runningRecording();
        recording.databaseName = details.DatabaseName;
        recording.operationId = operationId;
        recording.filePath = details.FilePath;
        recording.inProgress = inProgress;
        return recording;
    }
}

class debugAdvancedRecordTransactionCommands extends viewModelBase {

    private highlightDatabase: string;
    
    databaseNames = ko.observableArray<string>([]);
    databaseName = ko.observable<string>();
    outputFile = ko.observable<string>();
    
    runningTasks = ko.observableArray<runningRecording>([]);
    
    validationGroup: KnockoutValidationGroup;
    
    // marker for parent router
    static preventParentGrow = true;
    
    constructor() {
        super();
        
        this.initValidation();
    }
    
    private initValidation() {
        this.outputFile.extend({
            required: true
        });
        
        this.databaseName.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            databaseName: this.databaseName,
            outputFile: this.outputFile
        });
    }
    
    activate(args: { highlight: string }) {
        super.activate(args);
        
        this.highlightDatabase = args ? args.highlight : null;
        
        this.databaseNames(databasesManager.default.databases().map(x => x.name));
        
        this.registerDisposable(notificationCenter.instance.globalNotifications
            .subscribe(() => this.syncRunningRecordings()));
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.syncRunningRecordings();
    }

    private syncRunningRecordings() {
        const runningRecordCommands = notificationCenter.instance.globalNotifications()
            .filter(x => x instanceof operation && x.taskType() === "RecordTransactionCommands" && !x.isCompleted())
            .map(command => {
                const operation = command as operation;
                const description = operation.detailedDescription() as Raven.Server.Documents.Handlers.TransactionsRecordingHandler.RecordingDetails;
                const inProgress = ko.pureComputed(() => operation.status() === "InProgress");
                return runningRecording.for(description, operation.operationId(), inProgress);
            });
        
        this.runningTasks(runningRecordCommands);
        
        if (this.highlightDatabase) {
            const itemToHighLight = this.runningTasks().find(x => x.databaseName === this.highlightDatabase);
            if (itemToHighLight) {
                itemToHighLight.highlight(true);
            }
            
            // highlight only one time
            this.highlightDatabase = null;
        }
    }

    startRecording() {
        if (!this.isValid(this.validationGroup)) {
            return;
        }
        
        const db = databasesManager.default.getDatabaseByName(this.databaseName());
        new recordTransactionsCommand(db, this.outputFile())
            .execute();
    }
    
    stopRecording(databaseName: string) {
        const db = databasesManager.default.getDatabaseByName(databaseName);
        
        new stopRecordingTransactionsCommand(db)
            .execute();
    }
}

export = debugAdvancedRecordTransactionCommands;
