import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getFolderPathOptionsCommand extends commandBase {

    private readonly db: database;

    private readonly inputPath: string;
    
    private readonly nodeTag: string;

    private readonly isBackupFolder: boolean = false;

    private readonly connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType;

    private readonly credentials?: Raven.Client.Documents.Operations.Backups.BackupSettings;

    private constructor(
        db: database, 
        inputPath: string, 
        nodeTag: string,
        isBackupFolder = false, 
        connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType, 
        credentials?: Raven.Client.Documents.Operations.Backups.BackupSettings) {
        super();
        this.db = db;
        this.inputPath = inputPath;
        this.nodeTag = nodeTag;
        this.isBackupFolder = isBackupFolder;
        this.connectionType = connectionType;
        this.credentials = credentials;
    }

    private preparePayload() {
        switch (this.connectionType) {
            case "Local":
                return undefined;
            default:
                return JSON.stringify(this.credentials);
        }
    }
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.FolderPathOptions> {
        const args = {
            type: this.connectionType
        } as any;
        
        if (this.connectionType === "Local") {
            args.path = this.inputPath ? this.inputPath : "";
            args.backupFolder = this.isBackupFolder;
        }

        const url = this.db 
            ? endpoints.databases.studioDatabaseTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args)
            : endpoints.global.studioTasks.adminStudioTasksFolderPathOptions + this.urlEncodeArgs(args);
        
        return this.post<Raven.Server.Web.Studio.FolderPathOptions>(url, this.preparePayload(), this.db);
    }
    
    static forServerLocal(inputPath: string, isBackupFolder: boolean, nodeTag: string = null, db: database = null) {
        return new getFolderPathOptionsCommand(db, inputPath, nodeTag, isBackupFolder, "Local");
    }

    static forCloudBackup(credentials: Raven.Client.Documents.Operations.Backups.BackupSettings, type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType) {
        return new getFolderPathOptionsCommand(null, null, null, false, type, credentials);  
    }
}

export = getFolderPathOptionsCommand;
