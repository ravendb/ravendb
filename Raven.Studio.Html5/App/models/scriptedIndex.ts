import document = require("models/document");
import documentMetadata = require("models/documentMetadata");

class scriptedIndex extends document {

    static PREFIX = 'Raven/ScriptedIndexResults/';
    indexName = ko.observable<string>();
    indexScript = ko.observable<string>();
    deleteScript = ko.observable<string>();
    deleteLater = ko.observable<boolean>();

    constructor(dto: scriptedIndexDto) {
        super(dto);

        var scriptedIndexName = dto['@metadata']['@id'].slice(scriptedIndex.PREFIX.length);
        this.indexName(scriptedIndexName);
        this.indexScript(dto.IndexScript);
        this.deleteScript(dto.DeleteScript);
    }

    static emptyForIndex(indexName: string): scriptedIndex {
        var meta = [];
        meta['@id'] = this.PREFIX + indexName;
        meta['Raven-Entity-Name'] = 'ScriptedIndexResults';
        return new scriptedIndex({
            '@metadata': meta,
            IndexScript: "",
            DeleteScript: ""
        });
    }

    toDto(): scriptedIndexDto {
        var meta = this.__metadata.toDto();
        return {
            '@metadata': meta,
            IndexScript: this.indexScript(),
            DeleteScript: this.deleteScript()
        };
    }

    markToDelete() {
        this.indexScript("");
        this.deleteScript("");
        this.deleteLater(true);
    }

    cancelDeletion() {
        this.deleteLater(false);
    }

    isMarkedToDelete(): boolean {
        return this.deleteLater();
    }
}

export = scriptedIndex;