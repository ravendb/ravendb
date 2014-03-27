import scriptedIndex = require("models/scriptedIndex");
import document = require("models/document");

class scriptedIndexMap {

    private PREFIX = 'Raven/ScriptedIndexResults/';

    indexes = {};

    constructor(scriptedIndexes: scriptedIndex[]) {
        scriptedIndexes.forEach(index => {
            this.indexes[index.__metadata['id']] = index;
        });
    }

    getIndex(indexName: string): scriptedIndex {
        return this.indexes[this.PREFIX + indexName];
    }

    addEmptyIndex(indexName: string) {
        if (this.indexes[this.PREFIX + indexName]) {
            this.indexes[this.PREFIX + indexName].cancelDeletion();
        } else {
            this.indexes[this.PREFIX + indexName] = scriptedIndex.emptyForIndex(indexName);
        }
    }

    removeIndex(indexName: string) {
        this.indexes[this.PREFIX + indexName].markToDelete();
    }

    getIndexes(): Array<scriptedIndex> {
        return jQuery.map(this.indexes, function (value: scriptedIndex, index) {
            if (value) return value;
        });
    }
}

export = scriptedIndexMap;