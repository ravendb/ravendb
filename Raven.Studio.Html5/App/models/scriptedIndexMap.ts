import scriptedIndex = require("models/scriptedIndex");

class scriptedIndexMap {

    private PREFIX = 'Raven/ScriptedIndexResults/';

    indexes = {}

    constructor(scriptedIndexes: scriptedIndex[]) {
        scriptedIndexes.forEach(index => {
            this.indexes[index.__metadata['id']] = index;
        });
    }

    getIndex(indexName: string): scriptedIndex {
        return this.indexes[this.PREFIX + indexName];
    }

    addEmptyIndex(indexName: string) {
        this.indexes[this.PREFIX + indexName] = scriptedIndex.empty();
    }
}

export = scriptedIndexMap;