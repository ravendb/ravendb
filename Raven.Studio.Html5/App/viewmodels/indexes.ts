import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");

class indexes extends activeDbViewModelBase {

    indexGroups = ko.observableArray<{ entityName: string; indexes: indexStatisticsDto[] }>();
    
    activate(args) {
        super.activate(args);

        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: databaseStatisticsDto) => this.processDbStats(stats));
    }

    processDbStats(stats: databaseStatisticsDto) {
        stats.Indexes.forEach(i => this.putIndexIntoGroups(i));
    }

    putIndexIntoGroups(index: indexStatisticsDto) {
        if (index.ForEntityName.length === 0) {
            this.putIndexIntoGroupNamed(index, "Other");
        } else {
            index.ForEntityName.forEach(e => this.putIndexIntoGroupNamed(index, e));
        }
    }

    putIndexIntoGroupNamed(index: indexStatisticsDto, groupName: string) {
        var group = this.indexGroups.first(g => g.entityName === groupName);
        if (group) {
            group.indexes.push(index);
        } else {
            this.indexGroups.push({ entityName: groupName, indexes: [index] });
        }
    }

    navigateToQuery() {
        console.log("TODO: implement");
    }

    navigateToNewIndex() {
        console.log("TODO: implement");
    }

    collapseAll() {
        $(".index-group-content").collapse('hide');
    }

    expandAll() {
        $(".index-group-content").collapse('show');
    }

    deleteIdleIndexes() {
        
    }

    deleteDisabledIndexes() {
        // TODO: implement
    }

    deleteAbandonedIndexes() {
        // TODO: implement
    }
}

export = indexes;