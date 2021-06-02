/// <reference path="../../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

abstract class backupSettings {
    enabled = ko.observable<boolean>();

    connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType;
    
    isTestingCredentials = ko.observable<boolean>();
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;
    
    hasConfigurationScript = ko.observable<boolean>(false);
    
    configurationScript = {
        arguments: ko.observable<string>(),
        exec: ko.observable<string>(),
        timeoutInMs: ko.observable<number>(10000)
    };

    localConfigValidationGroup: KnockoutValidationGroup;
    configurationScriptValidationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;
    configurationScriptDirtyFlag: () => DirtyFlag;

    effectiveValidationGroup = ko.pureComputed<KnockoutValidationGroup>(() =>
        this.hasConfigurationScript() ? this.configurationScriptValidationGroup : this.localConfigValidationGroup);

    protected constructor(dto: Raven.Client.Documents.Operations.Backups.BackupSettings, connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType) {
        this.enabled(!dto.Disabled);
        this.connectionType = connectionType;
        
        this.hasConfigurationScript(!!dto.GetBackupConfigurationScript);
        if (dto.GetBackupConfigurationScript) {
            const configScript = dto.GetBackupConfigurationScript;
            this.configurationScript.arguments(configScript.Arguments);
            this.configurationScript.exec(configScript.Exec);
            this.configurationScript.timeoutInMs(configScript.TimeoutInMs);
        }
        
        this.initObservables();
        this.initConfigurationScriptValidation();
        
        this.configurationScriptDirtyFlag = new ko.DirtyFlag([
            this.hasConfigurationScript, 
            this.configurationScript.timeoutInMs, 
            this.configurationScript.exec,
            this.configurationScript.arguments
        ]);
    }

    initConfigurationScriptValidation() {
        this.configurationScript.exec.extend({
            required: true
        });
        
        this.configurationScript.timeoutInMs.extend({
            required: true
        });
        
        this.configurationScriptValidationGroup = ko.validatedObservable({
            arguments: this.configurationScript.arguments,
            exec: this.configurationScript.exec,
            timeoutInMs: this.configurationScript.timeoutInMs
        });
    }

    private initObservables() {
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
        
        this.hasConfigurationScript.subscribe(overrideWithScript => {
            if (overrideWithScript) {
                this.testConnectionResult(null);
            }
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.BackupSettings {
        return {
            Disabled: !this.enabled(),
            GetBackupConfigurationScript: this.hasConfigurationScript() ? this.configurationScriptToDto() : undefined
        }
    }
    
    private configurationScriptToDto() {
        return {
            Exec: this.configurationScript.exec(),
            Arguments: this.configurationScript.arguments(),
            TimeoutInMs: this.configurationScript.timeoutInMs()
        } as Raven.Client.Documents.Operations.Backups.GetBackupConfigurationScript;
    }
}

export = backupSettings;
