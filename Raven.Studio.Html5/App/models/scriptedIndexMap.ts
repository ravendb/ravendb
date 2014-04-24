import scriptedIndex = require("models/scriptedIndex");
import document = require("models/document");

class scriptedIndexMap {

    private PREFIX = 'Raven/ScriptedIndexResults/';
    indexes = {};
    activeScriptedIndexes = ko.observableArray<scriptedIndex>().extend({ required: true });
    
    constructor(scriptedIndexes: scriptedIndex[]) {
        scriptedIndexes.forEach(index => {
            this.indexes[index.__metadata['id']] = index;
            this.activeScriptedIndexes().push(index);
        });
    }

    getIndex(indexName: string): scriptedIndex {
        return this.indexes[this.PREFIX + indexName];
    }

    addEmptyIndex(indexName: string) {
        var activeScriptedIndex: scriptedIndex = this.indexes[this.PREFIX + indexName];
        if (activeScriptedIndex) {
            activeScriptedIndex.cancelDeletion();
        } else {
            activeScriptedIndex = scriptedIndex.emptyForIndex(indexName);
            this.indexes[this.PREFIX + indexName] = activeScriptedIndex;
        }
        this.activeScriptedIndexes().push(activeScriptedIndex);
        this.activeScriptedIndexes.notifySubscribers();
    }

    removeIndex(indexName: string) {
        var scriptedIndexToDelete: scriptedIndex = this.indexes[this.PREFIX + indexName];
        scriptedIndexToDelete.markToDelete();

        var index = this.activeScriptedIndexes().indexOf(scriptedIndexToDelete);
        if (index > -1) {
            this.activeScriptedIndexes().splice(index, 1);
            this.activeScriptedIndexes.notifySubscribers();
        }
    }

    deleteMarkToDeletedIndex(indexName: string) {
        delete this.indexes[indexName];
    }

    getIndexes(): Array<scriptedIndex> {
        return jQuery.map(this.indexes, function (value: scriptedIndex, index) {
            if (value) return value;
        });
    }
}

export = scriptedIndexMap;