import copyToClipboard = require("common/copyToClipboard");
import viewModelBase = require("viewmodels/viewModelBase");
import createSampleDataCommand = require("commands/database/studio/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/database/studio/createSampleDataClassCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import eventsCollector = require("common/eventsCollector");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import { highlight, languages } from "prismjs";

class createSampleData extends viewModelBase {
    
    view = require("views/database/tasks/createSampleData.html");

    classData = ko.observable<string>();
    canCreateSampleData = ko.observable<boolean>(false);
    justCreatedSampleData = ko.observable<boolean>(false);
    classesVisible = ko.observable<boolean>(false);

    classDataFormatted = ko.pureComputed(() => {
        return highlight(this.classData(), languages.javascript, "js");
    });

    constructor() {
        super();
        aceEditorBindingHandler.install();
    }

    generateSampleData() {
        eventsCollector.default.reportEvent("sample-data", "create");
        this.isBusy(true);

        const db = this.activeDatabase();
        
        new createSampleDataCommand(db)
            .execute()
            .done(() => {
                this.canCreateSampleData(false);
                this.justCreatedSampleData(true);
                this.checkIfRevisionsWasEnabled(db);
            })
            .always(() => this.isBusy(false));
    }
    
    private checkIfRevisionsWasEnabled(db: database) {
        if (!db.hasRevisionsConfiguration()) {
                new getDatabaseCommand(db.name)
                    .execute()
                    .done(dbInfo => {
                        if (dbInfo.HasRevisionsConfiguration) {
                            db.hasRevisionsConfiguration(true);

                            collectionsTracker.default.configureRevisions(db);
                        }
                    })
        }
    }
    

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('OGRN53');

        return $.when<any>(this.fetchSampleDataClasses(), this.fetchCollectionsStats());
    }

    showCode() {
        this.classesVisible(true);

        const $pageHostRoot = $("#page-host-root");
        const $sampleDataMain = $(".sample-data-main");

        $pageHostRoot.animate({
            scrollTop: $sampleDataMain.height()
        }, 'fast');
    }

    copyClasses() {
        eventsCollector.default.reportEvent("sample-data", "copy-classes");
        copyToClipboard.copy(this.classData(), "Copied C# classes to clipboard.");
    }

    private fetchCollectionsStats() {
        new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done(stats => this.onCollectionsFetched(stats));
    }

    private onCollectionsFetched(stats: collectionsStats) {
        const nonEmptyNonSystemCollectionsCount = stats
            .collections
            .filter(x => x.documentCount() > 0)
            .length;
        this.canCreateSampleData(nonEmptyNonSystemCollectionsCount === 0);
    }

    private fetchSampleDataClasses(): JQueryPromise<string> {
        return new createSampleDataClassCommand(this.activeDatabase())
            .execute()
            .done((results: string) => {
                this.classData(results);
            });
    }

    private urlForDatabaseDocuments() {
        return appUrl.forDocuments("", this.activeDatabase());
    }
}

export = createSampleData; 
