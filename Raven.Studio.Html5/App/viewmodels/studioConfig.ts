import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import documentClass = require("models/document");
import serverBuildReminder = require("common/serverBuildReminder");
import eventSourceSettingStorage = require("common/eventSourceSettingStorage");
import environmentColor = require("models/environmentColor");
import shell = require("viewmodels/shell");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    configDocument = ko.observable<documentClass>();
    warnWhenUsingSystemDatabase = ko.observable<boolean>(true);
    disableEventSource = ko.observable<boolean>(false);
    timeUntilRemindToUpgrade = ko.observable<string>();
    mute: KnockoutComputed<boolean>;

    public environmentColors: environmentColor[] = [
        new environmentColor("Default", "#f8f8f8", "#000000"),
        new environmentColor("Development", "#FFE6E6", "#000000"),
        new environmentColor("QA", "#CCFFCC", "#000000"),
        new environmentColor("Production", "#C2BCFF", "#000000")
    ]; 

    selectedColor = ko.observable<environmentColor>();


    setOptionStyle(option, item: environmentColor) {
        option.style.color = item.textColor;
        option.style.backgroundColor = item.backgroundColor;
    }

    timeUntilRemindToUpgradeMessage: KnockoutComputed<string>;


    private documentId = "Raven/StudioConfig";

    constructor() {
        super();
        this.systemDatabase = appUrl.getSystemDatabase();

        this.timeUntilRemindToUpgrade(serverBuildReminder.get());
        this.disableEventSource(eventSourceSettingStorage.get());
        this.mute = ko.computed(() => {
            var lastBuildCheck = this.timeUntilRemindToUpgrade();
            var timestamp = Date.parse(lastBuildCheck);
            var isLegalDate = !isNaN(timestamp);
            return isLegalDate;
        });
        this.timeUntilRemindToUpgradeMessage = ko.computed(() => {
            if (this.mute()) {
                var lastBuildCheck = this.timeUntilRemindToUpgrade();
                var lastBuildCheckMoment = moment(lastBuildCheck);
                return 'muted for another ' + lastBuildCheckMoment.add('days', 7).fromNow(true);
            }
            return 'mute for a week'; 
        });

        var color = this.environmentColors.filter((color) => color.name == shell.selectedEnvironmentColorStatic().name);
        this.selectedColor(!!color[0] ? color[0] : this.environmentColors[0]);

        var self = this;
        this.selectedColor.subscribe((newValue) => {
                self.setEnviromentColor(newValue);

        });


    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new getDocumentWithMetadataCommand(this.documentId, this.systemDatabase)
            .execute()
            .done((doc: documentClass) => {
                this.configDocument(doc);
                this.warnWhenUsingSystemDatabase(doc["WarnWhenUsingSystemDatabase"]);
            })
            .fail(() => this.configDocument(documentClass.empty()))
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('4J5OUB');
    }

    attached() {
        var self = this;
        $(window).bind('storage', (e: any) => {
            if (e.originalEvent.key == serverBuildReminder.localStorageName) {
                self.timeUntilRemindToUpgrade(serverBuildReminder.get());
            }
        });

       
    }

    setEnviromentColor(envColor: environmentColor) {
        var newDocument = this.configDocument();
        newDocument["EnvironmentColor"] = envColor.toDto();
        var saveTask = this.saveStudioConfig(newDocument);
        saveTask.done(() => shell.selectedEnvironmentColorStatic(this.selectedColor()));
    }

    setSystemDatabaseWarning(warnSetting: boolean) {
        if (this.warnWhenUsingSystemDatabase() !== warnSetting) {
            var newDocument = this.configDocument();
            this.warnWhenUsingSystemDatabase(warnSetting);
            newDocument["WarnWhenUsingSystemDatabase"] = warnSetting;
            var saveTask = this.saveStudioConfig(newDocument);
            saveTask.fail(() => this.warnWhenUsingSystemDatabase(!warnSetting));
        }
    }

    setEventSourceDisabled(setting: boolean) {
        this.disableEventSource(setting);
        eventSourceSettingStorage.setValue(setting);
    }

    setUpgradeReminder(upgradeSetting: boolean) {
        serverBuildReminder.mute(upgradeSetting);
    }

    saveStudioConfig(newDocument: documentClass) {
        var deferred = $.Deferred();

        require(["commands/saveDocumentCommand"], saveDocumentCommand => {
            var saveTask = new saveDocumentCommand(this.documentId, newDocument, this.systemDatabase).execute();
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