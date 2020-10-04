/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import generalUtils = require("common/generalUtils");
import shell = require("viewmodels/shell");
import getAllServerWideTasksCommand = require("commands/resources/getAllServerWideTasksCommand");

class ongoingTaskServerWideBackupListModel extends ongoingTaskListModel {    
    editUrl: KnockoutComputed<string>;

    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();
    fullBackupTypeName: KnockoutComputed<string>;
    
    retentionPolicyPeriod = ko.observable<string>();
    retentionPolicyDisabled = ko.observable<boolean>();
    retentionPolicyHumanized: KnockoutComputed<string>;
    
    backupDestinations = ko.observableArray<string>([]);
    backupDestinationsHumanized: KnockoutComputed<string>;
    textClass: KnockoutComputed<string>;
    
    isBackupEncrypted = ko.observable<boolean>();
    excludedDatabases = ko.observableArray<string>();

    constructor(dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask) {
        super();
        
        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        this.editUrl = ko.pureComputed(() => appUrl.forEditServerWideBackup(this.taskName()));

        this.retentionPolicyHumanized = ko.pureComputed(() => {
            return this.retentionPolicyDisabled() ? "No backups will be removed" : generalUtils.formatTimeSpan(this.retentionPolicyPeriod(), true);
        });
        
        this.backupDestinationsHumanized =  ko.pureComputed(() => {
            return this.backupDestinations().length ? this.backupDestinations().join(', ') : "No destinations defined";
        });

        this.fullBackupTypeName = ko.pureComputed(() => this.getBackupType(this.backupType(), true));
        
        this.textClass = ko.pureComputed(() => this.backupDestinations().length ? "text-details" : "text-warning")
    }

    // dto param is union-type only so that it compiles - due to class inheritance....
    update(dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask | Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask) {
        super.update({
            TaskName: dto.TaskName,
            TaskId: dto.TaskId,
            TaskState: dto.TaskState,
                       TaskConnectionStatus: null, // not relevant for this view
                       ResponsibleNode: null,      // not relevant for this view 
                       MentorNode: null,           // not relevant for this view 
                       } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTask );

        const serverWideBackupTask = dto as Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask;

        this.backupType(serverWideBackupTask.BackupType);
        this.backupDestinations(serverWideBackupTask.BackupDestinations);
        
        // Check backward compatibility
        this.retentionPolicyDisabled(serverWideBackupTask.RetentionPolicy ? serverWideBackupTask.RetentionPolicy.Disabled : true);
        this.retentionPolicyPeriod(serverWideBackupTask.RetentionPolicy ? serverWideBackupTask.RetentionPolicy.MinimumBackupAgeToKeep : "0.0:00:00");
        
        this.isBackupEncrypted(serverWideBackupTask.IsEncrypted);
        this.excludedDatabases(serverWideBackupTask.ExcludedDatabases);
    }

    private getBackupType(backupType: Raven.Client.Documents.Operations.Backups.BackupType, isFull: boolean): string {
        if (!isFull) {
            return "Incremental";
        }

        if (backupType === "Snapshot") {
            return "Snapshot";
        }

        return "Full";
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle();

        if (this.showDetails()) {
            this.refreshBackupInfo(true);
        } 
    }

    refreshBackupInfo(reportFailure: boolean) {
        if (shell.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>().fail();
        }

        return new getAllServerWideTasksCommand(this.taskName(), "Backup")
            .execute()
            .done((result: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult) =>
                this.update(result.Tasks[0]));
    }
}

export = ongoingTaskServerWideBackupListModel;
