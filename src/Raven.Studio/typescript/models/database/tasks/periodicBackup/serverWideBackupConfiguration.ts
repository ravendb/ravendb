/// <reference path="../../../../../typings/tsd.d.ts"/>
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import databasesManager = require("common/shell/databasesManager");
import jsonUtil = require("common/jsonUtil");

class serverWideBackupConfiguration extends periodicBackupConfiguration {
    
    excludeDatabases = ko.observable<boolean>();
    canAddDatabase: KnockoutComputed<boolean>;
    inputDatabaseToExclude = ko.observable<string>();
    
    databasesToExclude = ko.observableArray<string>();

    serverWideDirtyFlag: () => DirtyFlag;
    serverWideValidationGroup: KnockoutValidationGroup;

    constructor(databaseName: KnockoutObservable<string>,
                dto: Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration,
                serverLimits: periodicBackupServerLimitsResponse,
                encryptedDatabase: boolean,
                isServerWide: boolean) {

        super(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);

        this.databasesToExclude(dto.ExcludedDatabases || []);
        this.excludeDatabases(dto.ExcludedDatabases && dto.ExcludedDatabases.length > 0);
        
        this.initObservablesServerWide();
        this.initValidationServerWide();
    }

    private initObservablesServerWide() {
        this.canAddDatabase = ko.pureComputed(() => {
            const databaseToAdd = this.inputDatabaseToExclude();
            return databaseToAdd && !this.databasesToExclude().find(x => x === databaseToAdd);
        });
        
        this.serverWideDirtyFlag = new ko.DirtyFlag([
            this.excludeDatabases,
            this.databasesToExclude,
            this.dirtyFlag()
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidationServerWide() {
        this.databasesToExclude.extend({
            validation: [
                {
                    validator: () => !this.excludeDatabases() || this.databasesToExclude().length,
                    message: "No databases added"
                }
            ]
        });

        this.serverWideValidationGroup = ko.validatedObservable({
            databasesToExclude: this.databasesToExclude
        });
    }

    createDatabaseNameAutocompleter() {
        return ko.pureComputed(() => {
            const key = this.inputDatabaseToExclude();
            const excludedDatabases = this.databasesToExclude();

            const dbNames = databasesManager.default.databases()
                .map(x => x.name)
                .filter(x => !_.includes(excludedDatabases, x));

            if (key) {
                return dbNames.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return dbNames;
            }
        });
    }
    
    addDatabase() {
        this.addWithBlink(this.inputDatabaseToExclude());
    }

    addWithBlink(databaseName: string) { 
        this.databasesToExclude.unshift(databaseName);
        this.inputDatabaseToExclude("");
        $(".collection-list li").first().addClass("blink-style");
    }

    removeDatabase(databaseName: string) {
        this.databasesToExclude.remove(databaseName);
    }

    toDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration {
        const backupConfigurationDto = super.toDto();

        const dto = backupConfigurationDto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = this.excludeDatabases() ? this.databasesToExclude() : [];
        return dto;
    }

    static empty(databaseName: KnockoutObservable<string>, serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean, isServerWide: boolean): serverWideBackupConfiguration {
        const dto = periodicBackupConfiguration.emptyDto() as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = null;
        return new serverWideBackupConfiguration(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);
    }
}

export = serverWideBackupConfiguration;
