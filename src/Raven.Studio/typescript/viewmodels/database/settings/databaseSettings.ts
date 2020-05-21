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
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import saveDatabaseSettingsConfirm = require("viewmodels/database/settings/saveDatabaseSettingsConfirm");
import models = require("models/database/settings/databaseSettingsModels");
import popoverUtils = require("common/popoverUtils");
import genUtils = require("common/generalUtils");

type viewModeType = "summaryMode" | "editMode";

class categoryInfo {
    name = ko.observable<string>();
    customizedEntriesCount = ko.observable<number>();
    
    constructor (categoryName: string, numberOfCustomizedEntries: number) {
        this.name(categoryName);
        this.customizedEntriesCount(numberOfCustomizedEntries);
    }
}

class databaseSettings extends viewModelBase {
    
    allEntries = ko.observableArray<models.settingsEntry>([]);
    isAnyMatchingEntries: KnockoutComputed<boolean>;
    
    categoriesInfo = ko.observable<Array<categoryInfo>>();
    allCategoryNames: KnockoutComputed<Array<string>>;
    selectedCategory = ko.observable<string>();

    private categoriesGridController = ko.observable<virtualGridController<categoryInfo>>();
    private summaryGridController = ko.observable<virtualGridController<models.settingsEntry>>();

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
        this.filterKeys.throttle(500).subscribe(() => this.computeEntriesToShow());

        this.isAnyMatchingEntries = ko.pureComputed(() => !!this.allEntries().filter(x => x.showEntry()).length);
    }
    
    private computeEntriesToShow() {
        this.allEntries().forEach(entry => entry.showEntry(this.shouldShowEntry(entry)));
        
        if (this.viewMode() === "summaryMode") {
            this.summaryGridController().reset(false);
        }
    }

    private shouldShowEntry(entry: models.settingsEntry) {

        const categoryCondition =  (this.viewMode() === "editMode" && this.selectedCategory() === entry.data.Metadata.Category) ||
                                    this.viewMode() === "summaryMode";

        const filterCondition = !this.filterKeys() || this.entryContainsFilterText(entry);

        return categoryCondition && filterCondition;
    }
    
    private entryContainsFilterText(entry: models.settingsEntry) {
        const searchText = this.filterKeys().toLocaleLowerCase();
        return entry.keyName().toLocaleLowerCase().includes(searchText);
    }

    canActivate(args: any) {
        return $.when<any>(super.canActivate(args))
            .then(() => {
                const deferred = $.Deferred<canActivateResultDto>();

                this.isForbidden(!accessManager.default.operatorAndAbove());

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
        this.initSummaryGrid();
        this.processAfterFetch();
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
        return $.when<pagedResult<categoryInfo>>({
            items: this.categoriesInfo(),
            totalResultCount: this.categoriesInfo().length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }

    private selectActionHandler(categoryToShow: categoryInfo, event: JQueryEventObject) {
        event.preventDefault();
        this.setCategory(categoryToShow.name());
    }
    
    private setCategory(category: string) {
        this.selectedCategory(category);
        
        if (this.categoriesGridController()) {
            const categoryToSet = this.categoriesInfo().find(x => x.name() === category)
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
                        extraClass: (x) => x.effectiveValueInUseText() ? "value-has-content" : ""
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
                        extraClass: (x) => x.effectiveValue() ? "value-has-content" : ""
                    }),
                    new textColumn<models.settingsEntry>(summaryGrid, x => x.effectiveValueOrigin(), "Origin", "20%", {
                        sortable: "string",
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
                            return x.effectiveValue() ? classes : classes += " null-value";
                        }
                    })
                ]
            }
        });
    }
    
    private fetchSummaryData(): JQueryPromise<pagedResult<models.settingsEntry>> {
        let entriesForSummaryMode = _.filter(this.allEntries(), x => x.showEntry());

        entriesForSummaryMode = entriesForSummaryMode.reduce((acc,element) => {
            if (element.entryClassForSummaryMode() === "highlight-key") {
                return [element, ...acc];
            }
            return [...acc, element];
        }, []);
        
        return $.when<pagedResult<models.settingsEntry>>({
            items: entriesForSummaryMode,
            totalResultCount: entriesForSummaryMode.length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }

    private fetchDatabaseSettings(db: database, refresh: boolean = false): JQueryPromise<Raven.Server.Config.SettingsResult> {
        eventsCollector.default.reportEvent("database-settings", "get");

        return new getDatabaseSettingsCommand(db, refresh)
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
        let categoryWithError: string;
        
        this.allEntries().some(entry => {
            if (this.getEntryStateClass(entry) === "invalid-item") {
                return categoryWithError = entry.data.Metadata.Category;
            }
        });
        
        return categoryWithError;
    }
    
    save() {
        const categoryWithErrors = this.findFirstCategoryWithError();
        if (categoryWithErrors) {
            this.setCategory(categoryWithErrors);

            return;
        }

        const settingsToSave = this.allEntries().map(entry => {
            if (entry instanceof models.databaseEntry) {
                return entry.getEntrySetting();
            }
        }).filter(x => !!x);

        const settingsToSaveSorted = _.sortBy(settingsToSave, x => x.key);
        
        const saveSettingsModel = new saveDatabaseSettingsConfirm((settingsToSaveSorted), this.howToReloadDatabaseHtml);

        saveSettingsModel.result.done(result => {
            if (result.can) {
                this.spinners.save(true);

                new saveDatabaseSettingsCommand(this.activeDatabase(), settingsToSaveSorted)
                    .execute()
                    .done(() => this.exitEditMode())
                    .always(() => this.spinners.save(false));
                }
            });

        app.showBootstrapDialog(saveSettingsModel);
    }

    refresh() {
        this.spinners.refresh(true);

        this.fetchData(true)
            .always(() => this.spinners.refresh(false));
    }
    
    fetchData(refresh: boolean = false) {
        return this.fetchDatabaseSettings(this.activeDatabase(), refresh)
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
        
        const categories = this.allCategoryNames().map(x => new categoryInfo(x, this.getNumberOfCustomizedEntires(x)));
        this.categoriesInfo(categories);
        
        this.hasPendingValues(this.allEntries().some(entry => entry instanceof models.databaseEntry && entry.hasPendingContent()));
        
        this.summaryGridController().reset(true);
    }

    private getNumberOfCustomizedEntires(category: string) {
        let customizedEntries = 0;
        
        this.allEntries().forEach(x => {
            if (x instanceof models.databaseEntry && x.data.Metadata.Category === category && x.override()) {
                customizedEntries++;
            }
        });
        
        return customizedEntries;
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
        const text = `<div class="padding">
                         <div class='bg-warning text-warning padding padding-xs margin-right'><i class='icon-warning margin-right margin-right-sm'></i>You have unsaved changes !</div>
                         <div class="margin-top">Clicking <strong>OK</strong> will reload all database settings from the server <strong>without saving</strong> your changes.</div>
                      </div>`;

        return this.confirmationMessage("Are you sure?", text,
            {
                html: true,
                buttons: ["Cancel", "OK"]
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
            this.setupEditMode();
        } else {
            this.confirmationMessage("Are you an expert?",
                "Modify the database settings only if you know what you are doing!", { buttons: ["Cancel", "OK"] })
                .done(() => {
                    this.editModeHasBeenEntered = true;
                    this.viewMode("editMode");
                    this.initCategoryGrid();
                    this.setupEditMode(false);
                });
        }
    }
    
    private setupEditMode(reset: boolean = true) {
        this.dirtyFlag().reset();
        
        if (reset) {
            this.categoriesGridController().reset(false);
        }
        
        this.setCategory(this.selectedCategory() || this.allCategoryNames()[0]);
        this.computeEntriesToShow();
       
        this.allEntries().forEach(entry => {
            entry.showEntry(this.shouldShowEntry(entry));

            if (entry instanceof models.databaseEntry) {
                entry.override.subscribe((override) => {
                    
                    const category = this.categoriesInfo().find(x => x.name() === entry.data.Metadata.Category);
                    const currentCustomizedCount = category.customizedEntriesCount();
                    if (override) {
                        category.customizedEntriesCount(currentCustomizedCount+1)
                    } else {
                        category.customizedEntriesCount(currentCustomizedCount-1)
                    }
                    
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

    readonly howToReloadDatabaseHtml = `<h4 class="">There are two ways to reload the database:</h4>
                                        <ul>
                                            <li>
                                                <small>
                                                    Disable and then enable the database from the databases-list-view in the Studio.<br>
                                                    This will reload the database on all the cluster nodes immediately.
                                                </small>
                                            </li>
                                            <li class=\"margin-top margin-top-sm\">
                                                <small>
                                                    Restart RavenDB on all nodes.<br>
                                                    The database settings configuration will become effective per node that is restarted.
                                                </small>
                                            </li>
                                        </ul>`;
}

export = databaseSettings;
