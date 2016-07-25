import document = require("models/database/documents/document");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

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

        this.subscribeToObservable(this.indexScript);
        this.subscribeToObservable(this.deleteScript);
        this.indexName.subscribe(v => this.__metadata.id = scriptedIndex.PREFIX + v);
    }

    static emptyForIndex(indexName: string): scriptedIndex {
        var meta: any = [];
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

    private subscribeToObservable(observable: KnockoutObservable<string>) {
        observable.subscribe((newValue) => {
            var message = "";
            var currentEditor = aceEditorBindingHandler.currentEditor;
            if (currentEditor != undefined) {
                var textarea: any = $(currentEditor.container).find('textarea')[0];

                if (newValue === "") {
                    message = "Please fill out this field.";
                }
                textarea.setCustomValidity(message);
                setTimeout(() => {
                    var annotations = currentEditor.getSession().getAnnotations();
                    var isErrorExists = false;
                    for (var i = 0; i < annotations.length; i++) {
                        var annotationType = annotations[i].type;
                        if (annotationType === "error" || annotationType === "warning") {
                            isErrorExists = true;
                            break;
                        }
                    }
                    if (isErrorExists) {
                        message = "The script isn't a javascript legal expression!";
                        textarea.setCustomValidity(message);
                    }
                }, 700);
            }
        });
    }
}

export = scriptedIndex;
