import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import index = require("models/database/index/index");
import persistIndexCommand = require("commands/database/index/persistIndexCommand");

class statusDebugPersistAutoIndex extends viewModelBase {

    static selectIndexText = "Select index";

    onRamIndexes = ko.observableArray<index>([]);
    indexName = ko.observable<string>(statusDebugPersistAutoIndex.selectIndexText);

    buttonEnabled = ko.computed(() => {
        return this.indexName() != statusDebugPersistAutoIndex.selectIndexText;
    });

    persistIndex() {
        var indexName = this.indexName();
        new persistIndexCommand(indexName, this.activeDatabase())
            .execute()
            .done(() => {
                this.indexName(statusDebugPersistAutoIndex.selectIndexText);
                var index = this.onRamIndexes.first(i => i.name === indexName);
                if (index) {
                    this.onRamIndexes.remove(index);
                }
            });
    }

    setSelectedIndex(indexName: string) {
        this.indexName(indexName);
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
    }

    canActivate(args: any) {
        super.canActivate(args);

        var deferred = $.Deferred();

        $.when(this.fetchIndexes())
            .done(() => deferred.resolve({ can: true }))
            .fail(() => deferred.resolve({ can: false }));

        return deferred;
    }

    fetchIndexes() {
        return new getDatabaseStatsCommand(this.activeDatabase()).execute()
            .done((result: databaseStatisticsDto) => {
                this.onRamIndexes(result.Indexes.map(i => new index(i)).filter(i => i.isOnRam() != "false"));
            });
    }
}

export = statusDebugPersistAutoIndex;
