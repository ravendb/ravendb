import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import accessManager = require("common/shell/accessManager");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import models = require("models/database/settings/databaseSettingsModels");
import genUtils = require("common/generalUtils");
import messagePublisher = require("common/messagePublisher");
import getServerSettingsCommand = require("commands/maintenance/getServerSettingsCommand");
import { settingsEntry } from "models/database/settings/databaseSettingsModels";

class serverSettings extends viewModelBase {

    view = require("views/manage/serverSettings.html");
    
    allEntries = ko.observableArray<models.settingsEntry>([]);
    
    private gridController = ko.observable<virtualGridController<models.settingsEntry>>();
    private columnPreview = new columnPreviewPlugin<models.settingsEntry>();

    spinners = {
        refresh: ko.observable<boolean>(false),
    };

    isForbidden = ko.observable<boolean>(false);
    filterKeys = ko.observable<string>("");

    constructor() {
        super();
        
        this.bindToCurrentInstance("refresh");
        this.initializeObservables();
    }

    private initializeObservables() {
        this.filterKeys.throttle(500).subscribe(() => {
            this.computeEntriesToShow();
        });
    }
    
    private computeEntriesToShow() {
        this.allEntries().forEach(entry => {
            entry.showEntry(this.isEntryMatchingFilter(entry));
            entry.entryMatchesFilter(this.isEntryMatchingFilter(entry));
        });

        this.gridController().reset(false);
    }

    private isEntryMatchingFilter(entry: models.settingsEntry) {
        return !this.filterKeys() || this.entryContainsFilterText(entry);
    }
    
    private entryContainsFilterText(entry: models.settingsEntry) {
        const searchText = this.filterKeys().toLocaleLowerCase();
        return entry.keyName().toLocaleLowerCase().includes(searchText);
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.isClusterAdminOrClusterNode());

                if (this.isForbidden()) {
                    deferred.resolve({ can: true });
                } else {
                    this.fetchServerSettings()
                        .done(() => deferred.resolve({can: true}))
                        .fail(() => {
                            deferred.resolve({ redirect: appUrl.forDatabases() });
                        });
                }

                return deferred;
            });
    }

    compositionComplete() {
        super.compositionComplete();

        if (!this.isForbidden()) {
            this.initGrid();
            this.processAfterFetch();
        }
    }
    
    private initGrid() {
        const gridController = this.gridController();
        gridController.headerVisible(true);

        gridController.init(() => this.fetchSummaryData(), () => {
            return [
                new textColumn<models.settingsEntry>(gridController, x => x.keyName(), "Configuration Key", "30%", {
                    sortable: "string",
                    extraClass: x => x.effectiveValueOrigin() == "Server" ? "highlight-key" : ""
                }),
                new textColumn<models.settingsEntry>(gridController, x => x.effectiveValue(), "Effective Value", "40%", {
                    sortable: "string",
                    useRawValue: (x) => !x.hasAccess() || x.isSecured(),
                    extraClass: (x) => {
                        if (x.hasAccess()) {
                            return x.effectiveValueInUseText() ? "value-has-content" : "";
                        }
                        return "no-color";
                    }
                }),
                new textColumn<models.settingsEntry>(gridController, x => x.effectiveValueOrigin(), "Origin", "20%", {
                    sortable: "string",
                    title: (x) => {
                        switch (x.effectiveValueOrigin()) {
                            case "Server":
                                return "Value is configured in the settings.json file, overriding the default settings";
                            case "Default":
                                return "No customized value is set";
                        }
                    },
                    extraClass: (x) => {
                        let classes = "source-item ";
                        switch (x.effectiveValueOrigin()) {
                            case "Server":
                                classes += "source-item-server";
                                break;
                            case "Default":
                                classes += "source-item-default";
                                break;
                        }
                        return classes;
                    }
                })
            ]
        });

        this.columnPreview.install(".summary-list-container", ".js-summary-details-tooltip",
            (details: models.settingsEntry,
             column: textColumn<models.settingsEntry>,
             e: JQuery.TriggeredEvent, 
             onValue: (context: any, valueToCopy?: string) => void) => {
            
                if (column.header === "Configuration Key") {
                    onValue(details.descriptionHtml(), column.getCellValue(details));
                } else if (column.header !== "Origin") {
                    const value = column.getCellValue(details);
                    if (value) {
                        if (column.header.includes("Effective Value")) {
                            if (!details.hasAccess()) {
                                onValue("Unauthorized to access value!", "");
                                return;
                            } else if (details.isSecured()) {
                                onValue(settingsEntry.passwordBullets, "");
                                return;
                            }
                        }
                        onValue(genUtils.escapeHtml(value), value);
                    }
                }
            });
    }
    
    private fetchSummaryData(): JQueryPromise<pagedResult<models.settingsEntry>> {
        const topEntries = this.allEntries().filter(entry => entry.showEntry() && entry.effectiveValueOrigin() === "Server");
        const allOtherEntries = this.allEntries().filter(entry => entry.showEntry() && entry.effectiveValueOrigin() !== "Server");
        const entriesForSummaryMode = topEntries.concat(allOtherEntries);
        
        return $.when<pagedResult<models.settingsEntry>>({
            items: entriesForSummaryMode,
            totalResultCount: entriesForSummaryMode.length
        });
    }

    private fetchServerSettings(): JQueryPromise<Raven.Server.Config.SettingsResult> {
        return new getServerSettingsCommand()
            .execute()
            .done((result: Raven.Server.Config.SettingsResult) => {
                const settingsEntries = result.Settings.map(x => models.settingsEntry.getEntry(x));

                this.allEntries(_.sortBy(settingsEntries, x => x.keyName()));
            });
    }
    
    refresh() {
        this.spinners.refresh(true);

        this.fetchData()
            .done(() => messagePublisher.reportSuccess("Server Settings was successfully refreshed"))
            .always(() => this.spinners.refresh(false));
    }
    
    fetchData() {
        return this.fetchServerSettings()
            .done(() => this.processAfterFetch());
    }

    private processAfterFetch() {
        this.computeEntriesToShow();
        
        this.gridController().reset(true);
    }
}

export = serverSettings;
