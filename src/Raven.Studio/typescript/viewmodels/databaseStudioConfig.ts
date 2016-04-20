import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import documentClass = require("models/database/documents/document");
import serverBuildReminder = require("common/serverBuildReminder");
import environmentColor = require("models/resources/environmentColor");
import shell = require("viewmodels/shell");

class studioConfig extends viewModelBase {

 
    configDocument = ko.observable<documentClass>();

    environmentColors: environmentColor[] = [
        new environmentColor("Default", "#f8f8f8"),
        new environmentColor("Development", "#80FF80"),
        new environmentColor("Staging", "#F5824D"),
        new environmentColor("Production", "#FF8585")
    ];
    selectedColor = ko.observable<environmentColor>();

    private static documentId = shell.studioConfigDocumentId;

    constructor() {
        super();
        //this.systemDatabase = appUrl.getSystemDatabase();
        //this.timeUntilRemindToUpgrade(serverBuildReminder.get());
        /*var systemColor = shell.selectedEnvironmentColorStatic();

        debugger;

        //var color = this.environmentColors.filter((color) => color.name === shell.selectedEnvironmentColorStatic().name);
        var selectedColor = !!systemColor ? systemColor : this.environmentColors[0];;
        this.selectedColor(selectedColor);*/

        /**/
        var color = this.environmentColors.filter((color) => color.name === shell.selectedEnvironmentColorStatic().name);
        var selectedColor = !!color[0] ? color[0] : this.environmentColors[0];
        this.selectedColor(selectedColor);

        var self = this;
        this.selectedColor.subscribe((newValue) => self.setEnvironmentColor(newValue));
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        deffered.resolve({ can: true });
        this.configDocument(documentClass.empty());
        /*new getDocumentWithMetadataCommand(studioConfig.documentId, this.activeDatabase())
            .execute()
            .done((doc: documentClass) => {
            this.configDocument(doc);
        })
            .fail(() => this.configDocument(documentClass.empty()))
            .always(() => deffered.resolve({ can: true }));*/

        return deffered;
    }

    activate(args) {
        super.activate(args);
        //this.updateHelpLink("4J5OUB");
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
        var deferred = $.Deferred();

        require(["commands/saveDocumentCommand"], saveDocumentCommand => {
            var saveTask = new saveDocumentCommand(studioConfig.documentId, newDocument, this.activeDatabase()).execute();

            saveTask
                .done((saveResult: bulkDocumentDto[]) => {
                    this.configDocument(newDocument);
                    this.configDocument().__metadata['@etag'] = saveResult[0].Etag;
                    deferred.resolve();
                })
                .fail(() => deferred.reject());
        });

        return deferred;
    }
}

export = studioConfig;
