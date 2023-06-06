/// <reference path="../../../../../typings/tsd.d.ts"/>
import databasesManager = require("common/shell/databasesManager");
import jsonUtil = require("common/jsonUtil");

class serverWideExcludeModel {
    exclude = ko.observable<boolean>();
    isSnapshot = ko.observable<boolean>();
    canAddDatabase: KnockoutComputed<boolean>;
    inputDatabaseToExclude = ko.observable<string>();
    
    databasesToExclude = ko.observableArray<string>();
    shardedDatabaseNames = ko.observableArray<string>();

    dirtyFlag: () => DirtyFlag;

    constructor(excludedDatabases: string[], isSnapshot?: KnockoutObservable<boolean>) {

        this.isSnapshot = isSnapshot || ko.observable(false);
        this.databasesToExclude(excludedDatabases || []);
        this.exclude(excludedDatabases && excludedDatabases.length > 0);
        
        this.initObservables();
        this.initValidation();

        this.isSnapshot.subscribe(isSnapshot => {
            if (isSnapshot && this.shardedDatabaseNames().length > 0) {
                this.exclude(true);
            }
        })
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

        this.shardedDatabaseNames(databasesManager.default.databases().filter(x => x.isSharded()).map(x => x.name));
    }

    private initValidation() {
        this.databasesToExclude.extend({
            validation: [
                {
                    validator: () => !this.exclude() || this.databasesToExclude().length > 0 || (this.isSnapshot() && this.shardedDatabaseNames().length > 0),
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
                .filter(x => {
                    if (this.isSnapshot()) {
                        return !excludedDatabases.includes(x) && !this.shardedDatabaseNames().includes(x);
                    }
                    return !excludedDatabases.includes(x);
                });

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
        if (!this.exclude()) {
            return [];
        }

        if (!this.isSnapshot()) {
            return this.databasesToExclude();
        }

        return [...new Set([...this.databasesToExclude(), ...this.shardedDatabaseNames()])];
    }
}

export = serverWideExcludeModel;
