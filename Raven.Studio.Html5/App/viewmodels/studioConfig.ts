import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import documentClass = require("models/document");
import serverBuildReminder = require("common/serverBuildReminder");

class studioConfig extends viewModelBase {

    systemDatabase: database;
    configDocument = ko.observable<documentClass>();
    warnWhenUsingSystemDatabase = ko.observable<boolean>(true);
    timeUntilRemindToUpgrade = ko.observable<string>();
    mute: KnockoutComputed<boolean>;
    timeUntilRemindToUpgradeMessage: KnockoutComputed<string>;
    private documentId = "Raven/StudioConfig";

    constructor() {
        super();
        this.systemDatabase = appUrl.getSystemDatabase();

        this.timeUntilRemindToUpgrade(serverBuildReminder.get());
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

    attached() {
        var self = this;
        $(window).bind('storage', (e: any) => {
            if (e.originalEvent.key == serverBuildReminder.localStorageName) {
                self.timeUntilRemindToUpgrade(serverBuildReminder.get());
            }
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