import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import documentClass = require("models/database/documents/document");
import serverBuildReminder = require("common/serverBuildReminder");
import eventSourceSettingStorage = require("common/eventSourceSettingStorage");
import saveDocumentCommand = require("commands/database/documents/saveDocumentCommand");
import environmentColor = require("models/resources/environmentColor");
import shell = require("viewmodels/shell");
import numberFormattingStorage = require("common/numberFormattingStorage");
import license = require("models/auth/license");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    configDocument = ko.observable<documentClass>();
    warnWhenUsingSystemDatabase = ko.observable<boolean>(true);
    disableEventSource = ko.observable<boolean>(false);
    timeUntilRemindToUpgrade = ko.observable<string>();
    mute: KnockoutComputed<boolean>;
    isForbidden = ko.observable<boolean>();
    isReadOnly: KnockoutComputed<boolean>;
    browserFormatExample = 5050.99.toLocaleString();
    rawFormat = ko.observable<boolean>();
    isHotSpare: KnockoutComputed<boolean>;

    environmentColors: environmentColor[] = [
        new environmentColor("Default", "#f8f8f8"),
        new environmentColor("Development", "#80FF80", "#3D773D"),
        new environmentColor("Staging", "#F5824D"),
        new environmentColor("Production", "#FF8585")
    ];
    selectedColor = ko.observable<environmentColor>();

    timeUntilRemindToUpgradeMessage: KnockoutComputed<string>;
    private documentId = shell.studioConfigDocumentId;

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
                return "muted for another " + lastBuildCheckMoment.add("days", 7).fromNow(true);
            }
            return "mute for a week"; 
        });

        var color = this.environmentColors.filter((color) => color.name === shell.selectedEnvironmentColorStatic().name);
        var selectedColor = !!color[0] ? color[0] : this.environmentColors[0];
        this.selectedColor(selectedColor);
        
        var self = this;
        this.selectedColor.subscribe((newValue) => self.setEnvironmentColor(newValue));

        this.isForbidden((shell.isGlobalAdmin() || shell.canReadWriteSettings() || shell.canReadSettings()) === false);
        this.isReadOnly = ko.computed(() => shell.isGlobalAdmin() === false && shell.canReadWriteSettings() === false && shell.canReadSettings());

        this.rawFormat(numberFormattingStorage.shouldUseRaw());

        this.isHotSpare = ko.computed(() => license.isHotSpare());
    }

    canActivate(args): any {
        var deferred = $.Deferred();

        if (this.isForbidden() === false) {
            new getDocumentWithMetadataCommand(this.documentId, this.systemDatabase)
                .execute()
                .done((doc: documentClass) => {
                    this.configDocument(doc);
                    this.warnWhenUsingSystemDatabase(doc["WarnWhenUsingSystemDatabase"]);
                })
                .fail(() => this.configDocument(documentClass.empty()))
                .always(() => deferred.resolve({ can: true }));
        } else {
            deferred.resolve({ can: true });
        }

        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("4J5OUB");
    }

    attached() {
        super.attached();
        var self = this;
        $(window).bind('storage', (e: any) => {
            if (e.originalEvent.key === serverBuildReminder.localStorageName) {
                self.timeUntilRemindToUpgrade(serverBuildReminder.get());
            }
        });

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
            shell.selectedEnvironmentColorStatic(this.selectedColor());
            this.pickColor();
        });
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

    private pickColor() {
        $("#select-color button").css("backgroundColor", this.selectedColor().backgroundColor);
    }

    setEventSourceDisabled(setting: boolean) {
        this.disableEventSource(setting);
        eventSourceSettingStorage.setValue(setting);
    }

    setUpgradeReminder(upgradeSetting: boolean) {
        serverBuildReminder.mute(upgradeSetting);
    }

    setNumberFormat(raw: boolean) {
        this.rawFormat(raw);
        numberFormattingStorage.save(raw);
    }

    saveStudioConfig(newDocument: documentClass) {
        return new saveDocumentCommand(this.documentId, newDocument, this.systemDatabase)
            .execute()
            .done((saveResult: bulkDocumentDto[]) => {
                this.configDocument(newDocument);
                this.configDocument().__metadata['etag'] = saveResult[0].Etag;
            });
    }
}

export = studioConfig;
