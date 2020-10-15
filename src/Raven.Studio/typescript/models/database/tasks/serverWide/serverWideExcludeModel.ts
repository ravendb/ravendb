/// <reference path="../../../../../typings/tsd.d.ts"/>
import databasesManager = require("common/shell/databasesManager");
import jsonUtil = require("common/jsonUtil");

class serverWideExcludeModel {
    exclude = ko.observable<boolean>();
    canAddDatabase: KnockoutComputed<boolean>;
    inputDatabaseToExclude = ko.observable<string>();
    
    databasesToExclude = ko.observableArray<string>();

    dirtyFlag: () => DirtyFlag;

    constructor(excludedDatabases: string[]) {

        this.databasesToExclude(excludedDatabases || []);
        this.exclude(excludedDatabases && excludedDatabases.length > 0);
        
        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.canAddDatabase = ko.pureComputed(() => {
            const databaseToAdd = this.inputDatabaseToExclude();
            return databaseToAdd && !this.databasesToExclude().find(x => x === databaseToAdd);
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.exclude,
            this.databasesToExclude
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.databasesToExclude.extend({
            validation: [
                {
                    validator: () => !this.exclude() || this.databasesToExclude().length,
                    message: "No databases added"
                }
            ]
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

    toDto() : string[] {
        return this.exclude() ? this.databasesToExclude() : [];
    }
}

export = serverWideExcludeModel;
