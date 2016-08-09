import viewModelBase = require("viewmodels/viewModelBase");
import documentClass = require("models/database/documents/document");
import environmentColor = require("models/resources/environmentColor");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import shell = require("viewmodels/shell");
import license = require("models/auth/license");

class studioConfig extends viewModelBase {
 
    configDocument = ko.observable<documentClass>();
    isHotSpare: KnockoutComputed<boolean>;

    environmentColors: environmentColor[] = [
        new environmentColor("Default", "#f8f8f8"),
        new environmentColor("Development", "#80FF80", "#3D773D"),
        new environmentColor("Staging", "#F5824D"),
        new environmentColor("Production", "#FF8585")
    ];
    selectedColor = ko.observable<environmentColor>();

    private static documentId = shell.studioConfigDocumentId;

    constructor() {
        super();
        var color = this.environmentColors.filter((color) => color.name === shell.selectedEnvironmentColorStatic().name);
        var selectedColor = !!color[0] ? color[0] : this.environmentColors[0];
        this.selectedColor(selectedColor);

        var self = this;
        this.selectedColor.subscribe((newValue) => self.setEnvironmentColor(newValue));

        this.isHotSpare = ko.computed(() => license.isHotSpare());
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        deffered.resolve({ can: true });
        this.configDocument(documentClass.empty());

        return deffered;
    }

    attached() {
        super.attached();

        $("select").selectpicker();
        this.pickColor();

        $("#select-color li").each((index, element) => {
            var color = this.environmentColors[index];
            $(element).css("backgroundColor", color.backgroundColor);
        });
    }

    setEnvironmentColor(envColor: environmentColor) {
        var newDocument = this.configDocument();
        newDocument["EnvironmentColor"] = envColor.toDto();
        var saveTask = this.saveStudioConfig(newDocument);
        saveTask.done(() => {
            this.pickColor();
            shell.selectedEnvironmentColorStatic(envColor);
        });
    }

    private pickColor() {
        $("#select-color button").css("backgroundColor", this.selectedColor().backgroundColor);
    }

    saveStudioConfig(newDocument: documentClass) {
        return new saveDocumentCommand(studioConfig.documentId, newDocument, this.activeDatabase())
            .execute()
            .done((saveResult: bulkDocumentDto[]) => {
                this.configDocument(newDocument);
                this.configDocument().__metadata['@etag'] = saveResult[0].Etag;
            });
    }
}

export = studioConfig;
