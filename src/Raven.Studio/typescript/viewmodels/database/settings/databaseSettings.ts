import app = require("durandal/app");
import appUrl = require("common/appUrl");
import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");
import accessManager = require("common/shell/accessManager");
import eventsCollector = require("common/eventsCollector");
import jsonUtil = require("common/jsonUtil");
import getDatabaseSettingsCommand = require("commands/database/settings/getDatabaseSettingsCommand");
import saveDatabaseSettingsCommand = require("commands/database/settings/saveDatabaseSettingsCommand");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import saveDatabaseSettingsConfirm = require("viewmodels/database/settings/saveDatabaseSettingsConfirm");
import models = require("models/database/settings/databaseSettingsModels");
import popoverUtils = require("common/popoverUtils");
import genUtils = require("common/generalUtils");
import messagePublisher = require("common/messagePublisher");

type viewModeType = "summaryMode" | "editMode";

class categoryInfo {
    name = ko.observable<string>();
    customizedEntriesCount: KnockoutComputed<number>;
    
    constructor (categoryName: string) {
        this.name(categoryName);
    }
}

class databaseSettings extends viewModelBase {
    
    allEntries = ko.observableArray<models.settingsEntry>([]);
    isAnyMatchingEntries: KnockoutComputed<boolean>;
    
    categoriesInfo = ko.observable<Array<categoryInfo>>();
    filteredCategories = ko.observable<Array<categoryInfo>>();
    
    allCategoryNames: KnockoutComputed<Array<string>>;
    selectedCategory = ko.observable<string>();

    private categoriesGridController = ko.observable<virtualGridController<categoryInfo>>();
    private summaryGridController = ko.observable<virtualGridController<models.settingsEntry>>();
    private columnPreview = new columnPreviewPlugin<models.settingsEntry>();

    viewMode = ko.observable<viewModeType>("summaryMode");
    editModeHasBeenEntered = false;

    isSaveEnabled: KnockoutComputed<boolean>;

    spinners = {
        refresh: ko.observable<boolean>(false),
        save: ko.observable<boolean>(false)
    };

    isForbidden = ko.observable<boolean>(false);
    filterKeys = ko.observable<string>("");
    hasPendingValues = ko.observable<boolean>(false);

    constructor() {
        super();
        
        this.bindToCurrentInstance("save", "refresh", "switchToEditMode", "exitEditMode");
        this.initializeObservables();
    }

    private initializeObservables() {

        this.initializeDirtyFlag();
        
        this.isSaveEnabled = ko.pureComputed(() => {
            const isDirty = this.dirtyFlag().isDirty();
            return isDirty && !this.spinners.save();
        });

        this.selectedCategory.subscribe(() => this.computeEntriesToShow());
        
        this.filterKeys.throttle(500).subscribe(() => {
            this.computeEntriesToShow();
            
            if (this.filterKeys() && this.filteredCategories() && this.filteredCategories().length) {
                this.setCategory(this.filteredCategories()[0].name());
            } else {
                this.setCategory(this.allCategoryNames()[0]);
            }
        });

        this.isAnyMatchingEntries = ko.pureComputed(() => !!this.allEntries().filter(x => x.showEntry()).length);
    }
    
    private computeEntriesToShow() {
        this.allEntries().forEach(entry => {
            entry.showEntry(this.shouldShowEntry(entry));
            entry.entryMatchesFilter(this.isEntryMatchingFilter(entry));
        });

        if (this.viewMode() === "summaryMode") {
            this.summaryGridController().reset(false);
        }

        if (this.viewMode() === "editMode") {
            this.categoriesGridController().reset(false);
        }
    }

    private shouldShowEntry(entry: models.settingsEntry) {

        const categoryCondition =  (this.viewMode() === "editMode" && this.selectedCategory() === entry.data.Metadata.Category) ||
                                    this.viewMode() === "summaryMode";

        const filterCondition = this.isEntryMatchingFilter(entry);

        return categoryCondition && filterCondition;
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

                this.isForbidden(!accessManager.default.isOperatorOrAbove());

                if (this.isForbidden()) {
                    deferred.resolve({can: true});
                } else {
                    const db: database = this.activeDatabase();
                    this.fetchDatabaseSettings(db)
                        .done(() => deferred.resolve({can: true}))
                        .fail((response: JQueryXHR) => {
                            deferred.resolve({redirect: appUrl.forStatus(db)});
                        });
                }

                return deferred;
            });
    }

    compositionComplete() {
        super.compositionComplete();
        
        if (!this.isForbidden()) {
            this.initSummaryGrid();
            this.processAfterFetch();
        }
    }
    
    private initCategoryGrid() {
        const categoriesGrid = this.categoriesGridController();
        categoriesGrid.headerVisible(true);

        categoriesGrid.init(() => this.fetchCategoriesData(), () =>
            [
                new hyperlinkColumn<categoryInfo>(categoriesGrid, x => this.getCategoryHtml(x), x => appUrl.forDatabaseSettings(this.activeDatabase()), "Category", "90%",
                    {
                        useRawValue: () => true,
                        sortable: "string",
                        handler: (categoryToShow, event) => this.selectActionHandler(categoryToShow, event)
                    }),
            ]);
    }

    private fetchCategoriesData(): JQueryPromise<pagedResult<categoryInfo>> {
        this.filteredCategories(this.categoriesInfo().filter(category => {
            return this.shouldShowCategory(category);
        }));
        
        return $.when<pagedResult<categoryInfo>>({
            items: this.filteredCategories(),
            totalResultCount: this.filteredCategories().length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }
    
    private shouldShowCategory(category: categoryInfo) {
        const entriesInCategory = this.allEntries().filter(entry => entry.data.Metadata.Category === category.name());

        for (let i = 0; i < entriesInCategory.length; i++) {
            if (entriesInCategory[i].entryMatchesFilter()) {
                return true; 
            }
        }
        
        return false;
    }

    private selectActionHandler(categoryToShow: categoryInfo, event: JQueryEventObject) {
        event.preventDefault();
        this.setCategory(categoryToShow.name());
    }
    
    private setCategory(category: string) {
        this.selectedCategory(category);
        
        if (this.categoriesGridController()) {
            const categoryToSet = this.filteredCategories().find(x => x.name() === category)
            this.categoriesGridController().setSelectedItems([categoryToSet]);
        }
    }

    private getCategoryHtml(category: categoryInfo) {
        const statePart = this.getCategoryStateClass(category.name());
        
        const namePart = `<div class="use-parent-action ${statePart}">${genUtils.escapeHtml(category.name())}</div>`;
        
        const countPart = `<div class="use-parent-action label label-default customized-entries-label" title="Number of customized entries in category">
                               ${category.customizedEntriesCount()}
                           </div>`;
        
        return category.customizedEntriesCount() ? namePart + countPart : namePart;
    }
    
    private getCategoryStateClass(category: string) {
        let state: string;
        let errorState: string;

        this.allEntries().filter(entry => entry.data.Metadata.Category === category).forEach(entry => {
            const entryState = this.getEntryStateClass(entry);
            
            if (entryState === "invalid-item") {
                errorState = entryState;
            }
    
            if (entryState === "dirty-item") {
                state = entryState;
            }
        });
        
        return errorState || state || "";
    }
    
    private getEntryStateClass(entry: models.settingsEntry) {
        let stateClass = "";

        if (entry instanceof models.databaseEntry) {
            if (entry.override() && !this.isValid(entry.validationGroup)) {
                stateClass = "invalid-item";
            } else if (entry.entryDirtyFlag().isDirty()) {
                stateClass = "dirty-item";
            }
        }
        
        return stateClass;
    }
    
    private initSummaryGrid() {
        const summaryGrid = this.summaryGridController();
        summaryGrid.headerVisible(true);

        summaryGrid.init(() => this.fetchSummaryData(), () => {
            if (this.hasPendingValues()) {
                return [
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.keyName(), "Configuration Key", "30%", {
                        sortable: "string",
                        extraClass: x => x.entryClassForSummaryMode()
                    }),
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.effectiveValueInUseText(), "Effective Value in Use", "30%", {
                        sortable: "string",
                        useRawValue: (x) => !x.hasAccess(),
                        extraClass: (x) => {
                            if (x.hasAccess()) {
                                return x.effectiveValueInUseText() ? "value-has-content" : "";
                            }
                            return "no-color";
                        }
                    }),
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.pendingValueText() , "Pending Value", "30%", {
                        sortable: "string",
                        extraClass: (x) => x.pendingValueText() ? "value-has-content" : ""
                    })
                ]
            } else {
                return [
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.keyName(), "Configuration Key", "30%", {
                        sortable: "string",
                        extraClass: x => x.entryClassForSummaryMode()
                    }),
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.effectiveValue(), "Effective Value", "40%", {
                        sortable: "string",
                        useRawValue: (x) => !x.hasAccess(),
                        extraClass: (x) => {
                            if (x.hasAccess()) {
                                return x.effectiveValueInUseText() ? "value-has-content" : "";
                            }
                            return "no-color";
                        }
                    }),
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.effectiveValueOrigin(), "Origin", "20%", {
                        sortable: "string",
                        title: (x) => {
                            switch (x.effectiveValueOrigin()) {
                                case "Database":
                                    return "Value is configured in the database record, overriding the server & default settings";
                                case "Server":
                                    return "Value is configured in the settings.json file, overriding the default settings";
                                case "Default":
                                    return "No customized value is set";
                            }
                        },
                        extraClass: (x) => {
                            let classes = "source-item ";
                            switch (x.effectiveValueOrigin()) {
                                case "Database":
                                    classes += "source-item-database";
                                    break;
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
            }
        });

        this.columnPreview.install(".summary-list-container", ".js-summary-details-tooltip",
            (details: models.settingsEntry,
             column: textColumn<models.settingsEntry>,
             e: JQueryEventObject,
             onValue: (context: any, valueToCopy?: string) => void) => {
                if (column.header !== "Origin") {
                    let value = column.getCellValue(details);
                    if (value) {
                        if (column.header.includes("Effective Value") && !details.hasAccess()) {
                            value = "Unauthorized to access value!";
                        }
                        onValue(genUtils.escapeHtml(value), value);
                    }
                }
            });
    }
    
    private fetchSummaryData(): JQueryPromise<pagedResult<models.settingsEntry>> {
        const topEntries = this.allEntries().filter(entry => entry.showEntry() && entry.entryClassForSummaryMode() === "highlight-key").sort();
        const allOtherEntries = this.allEntries().filter(entry => entry.showEntry() && entry.entryClassForSummaryMode() !== "highlight-key");
        const entriesForSummaryMode = topEntries.concat(allOtherEntries);
        
        return $.when<pagedResult<models.settingsEntry>>({
            items: entriesForSummaryMode,
            totalResultCount: entriesForSummaryMode.length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }

    private fetchDatabaseSettings(db: database): JQueryPromise<Raven.Server.Config.SettingsResult> {
        eventsCollector.default.reportEvent("database-settings", "get");

        return new getDatabaseSettingsCommand(db)
            .execute()
            .done((result: Raven.Server.Config.SettingsResult) => {
                
                const settingsEntries = result.Settings.map(x => {
                    const rawEntry = x as Raven.Server.Config.ConfigurationEntryDatabaseValue;
                    return models.settingsEntry.getEntry(rawEntry);
                });

                this.allEntries(_.sortBy(settingsEntries, x => x.keyName()));
            });
    }
    
    private findFirstCategoryWithError() {
        return this.allEntries().find(entry => this.getEntryStateClass(entry) === "invalid-item")?.data.Metadata.Category;
    }
    
    save() {
        const categoryWithErrors = this.findFirstCategoryWithError();
        if (categoryWithErrors) {
            this.setCategory(categoryWithErrors);

            return;
        }
        
        const settingsToSave = this.allEntries()
            .filter(entry => entry instanceof models.databaseEntry && entry.override())
            .map(entry => { 
                return { key: entry.keyName(), value: entry.effectiveValue() };
            });

        const settingsToSaveSorted = _.sortBy(settingsToSave, x => x.key);
      
        const settingsToSaveObject = settingsToSaveSorted.reduce((acc, item) => {
            acc[item.key] = item.value;
            return acc;
        }, {} as Record<string, string>);

        const saveSettingsModel = new saveDatabaseSettingsConfirm(settingsToSaveObject, this.howToReloadDatabaseHtml);

        saveSettingsModel.result.done(result => {
            if (result.can) {
                this.spinners.save(true);

                new saveDatabaseSettingsCommand(this.activeDatabase(), settingsToSaveObject)
                    .execute()
                    .done(() => this.exitEditMode())
                    .always(() => this.spinners.save(false));
                }
            });

        app.showBootstrapDialog(saveSettingsModel);
    }

    refresh() {
        this.spinners.refresh(true);

        this.fetchData()
            .done(() =>  messagePublisher.reportSuccess("Database Settings was successfully refreshed"))
            .always(() => this.spinners.refresh(false));
    }
    
    fetchData() {
        return this.fetchDatabaseSettings(this.activeDatabase())
            .done(() => this.processAfterFetch());
    }

    private processAfterFetch() {
        $('#database-settings-view [data-toggle="tooltip"]').tooltip();
        
        $('.description').on("mouseenter", event => {
            const target = $(event.currentTarget);
            const targetEntry = ko.dataFor(target[0]) as models.settingsEntry;

            if (!target.data('bs.popover')) {
                popoverUtils.longWithHover(target, {
                    content: `<div class="description-text">
                                <strong>${genUtils.escapeHtml(targetEntry.keyName())}</strong>
                                <small>${targetEntry.descriptionHtml()}</small>
                             </div>`,
                    placement: "bottom",
                    container: "#database-settings-view"
                })
            }

            target.popover('show');
        });

        popoverUtils.longWithHover($("#pendingValuesWarning"),
            {
                content: `<div class="margin-left margin-right margin-right-lg">${this.howToReloadDatabaseHtml}</div>`,
                placement: 'bottom'
            });
        
        this.computeEntriesToShow();
        
        this.allCategoryNames = ko.pureComputed(() => {
            return _.uniq(this.allEntries().map(entry => entry.data.Metadata.Category)).sort();
        });
        
        const categories = this.allCategoryNames().map(x => new categoryInfo(x));
        this.categoriesInfo(categories);
        
        this.hasPendingValues(this.allEntries().some(entry => entry instanceof models.databaseEntry && entry.hasPendingContent()));
        
        this.summaryGridController().reset(true);
    }
    
    cancelEdit() {
        if (this.dirtyFlag().isDirty()) {
            this.confirmUnsavedchanges()
                .then(result => {
                    if (result.can) {
                        this.exitEditMode();
                    }
                });
        } else {
            this.exitEditMode();
        }
    }

    confirmUnsavedchanges() {
        const text = `<div class="margin-top">You have unsaved changes. How do you want to proceed?</div>`;
        return this.confirmationMessage("Unsaved changes", text,
            {
                html: true,
                buttons: ["Stay in edit mode", "Discard changes"]
            });
    }
    
    exitEditMode() {
        this.viewMode("summaryMode");
        this.fetchData();
        this.computeEntriesToShow();
    }
    
    switchToEditMode() {
        if (this.editModeHasBeenEntered) {
            this.viewMode("editMode");
            this.setupEditMode(false);
        } else {
            const text = `<div class="margin-top">Modify the database settings only if you know what you are doing.</div>`;
            this.confirmationMessage("Are you an expert?", text, { html: true, buttons: ["Cancel", "OK"] })
                .done(() => {
                    this.editModeHasBeenEntered = true;
                    this.viewMode("editMode");
                    this.setupEditMode(true);
                });
        }
    }
    
    private setupEditMode(firstTime: boolean) {
        this.dirtyFlag().reset();

        this.categoriesInfo().forEach(category => {
            category.customizedEntriesCount = ko.pureComputed<number>(() => {
                return this.allEntries().filter(entry => entry.data.Metadata.Category === category.name() &&
                    entry instanceof models.databaseEntry &&
                    entry.override()).length;
            });
        });
        
        if (firstTime) {
            this.initCategoryGrid();
        } else {
            this.categoriesGridController().reset(false);
        }
        
        this.computeEntriesToShow();
        
        if (this.filteredCategories() && this.filteredCategories().length) {
            this.setCategory(this.filteredCategories()[0].name());
        }
       
        this.allEntries().forEach(entry => {
            if (entry instanceof models.databaseEntry) {
                entry.override.subscribe((override) => {
                    this.categoriesGridController().reset(false);
                    this.setCategory(entry.data.Metadata.Category);
                });
                
                entry.customizedDatabaseValue.throttle(300).subscribe(() => {
                    this.categoriesGridController().reset(false);
                });
            }
        });
    }

    private initializeDirtyFlag() {
        
        const hasAnyDirtyField = ko.pureComputed(() => {
            let anyDirty = false;
            
            this.allEntries().forEach(entry => {
                if (entry instanceof models.databaseEntry && entry.entryDirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
            
            return anyDirty;
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            hasAnyDirtyField
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    readonly howToReloadDatabaseHtml = `<h4>There are two ways to reload the database:</h4>
                                        <ul>
                                            <li>
                                                <small>
                                                    Disable and then enable the database from the databases view in the Studio.<br>
                                                    This will reload the database on all the cluster nodes immediately.
                                                </small>
                                            </li>
                                            <li class="margin-top margin-top-sm">
                                                <small>
                                                    Restart RavenDB on all nodes.<br>
                                                    The database settings configuration will become effective per node that is restarted.
                                                </small>
                                            </li>
                                        </ul>`;
}

export = databaseSettings;
