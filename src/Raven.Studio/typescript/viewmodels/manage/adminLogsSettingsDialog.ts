import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getAdminLogsSettingsCommand = require("commands/maintenance/getAdminLogsSettingsCommand");
import saveAdminLogsSettingsCommand = require("commands/maintenance/saveAdminLogsSettingsCommand");
import genUtils = require("common/generalUtils");

class adminLogsSettingsDialog extends dialogViewModelBase {

    possibleLogModes: Array<Sparrow.Logging.LogMode> = ["Operations", "Information"];
    currentServerMode = ko.observable<Sparrow.Logging.LogMode>();
    selectedMode = ko.observable<Sparrow.Logging.LogMode>();
    
    fullPath = ko.observable<string>();
    retentionTime = ko.observable<string>();
    retentionSize = ko.observable<Sparrow.Size>();
    compress: boolean;

    retentionTimeText: KnockoutComputed<string>;
    retentionSizeText: KnockoutComputed<Sparrow.Size>;
    
    constructor() {
        super();
        this.bindToCurrentInstance( "setAdminLogMode");
        this.getAdminLogsSettings(true);
        
        this.retentionTimeText = ko.pureComputed(() => {
           return this.retentionTime() ? genUtils.formatTimeSpan(this.retentionTime()) : "Unlimited";
        });
        
        this.retentionSizeText = ko.pureComputed(() => {
            return this.retentionSize() ? genUtils.formatBytesToSize(this.retentionSize() as number) : "Unlimited";
        });
    }
    
    private getAdminLogsSettings(useServerMode: boolean) {
        return new getAdminLogsSettingsCommand().execute()
            .done(result => {
                this.currentServerMode(result.CurrentMode);
                if (useServerMode) {
                    this.selectedMode(result.CurrentMode);
                }
                
                this.fullPath(result.Path);
                this.retentionTime(result.RetentionTime);
                this.retentionSize(result.RetentionSize);
                this.compress = result.Compress;
        });
    }
    
    setAdminLogMode(newMode: Sparrow.Logging.LogMode) {
        this.selectedMode(newMode);
        
        // First get updated with current server settings
        this.getAdminLogsSettings(false).done(() => {
            // Set the new mode only...
            new saveAdminLogsSettingsCommand(newMode, this.retentionTime(), this.retentionSize(), this.compress).execute()
                .done(() => this.currentServerMode(newMode))
                .fail(() => this.selectedMode(this.currentServerMode()));
        });
    }
    
    cancel() {
        dialog.close(this);
    }
}

export = adminLogsSettingsDialog;
