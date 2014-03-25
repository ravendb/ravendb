import document = require("models/document");
import documentMetadata = require("models/documentMetadata");

class scriptedIndex extends document {

    public metadata: documentMetadata;

    indexScript = ko.observable<string>();
    deleteScript = ko.observable<string>();

    constructor(dto: scriptedIndexDto) {

        super(dto);

        this.indexScript(dto.IndexScript);
        this.deleteScript(dto.DeleteScript);

        this.metadata = new documentMetadata(dto['@metadata']);
    }

    static empty(): scriptedIndex {
        return new scriptedIndex({
            IndexScript: "",
            DeleteScript: ""
        });
    }

    toDto(): scriptedIndexDto {
        var meta = this.__metadata.toDto();
        meta['@id'] = "Raven/ScriptedIndexResults/" + "TEST";
        return {
            '@metadata': meta,
            IndexScript: this.indexScript(),
            DeleteScript: this.deleteScript()
        };
    }

}

export = scriptedIndex;