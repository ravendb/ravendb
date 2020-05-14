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
type entriesGroupType = "allEntries" | "databaseEntries" | "customizedDatabaseEntries";

class databaseSettings extends viewModelBase {
    
    allEntries = ko.observableArray<models.settingsEntry>([]);
    groupToShow = ko.observable<entriesGroupType>("allEntries");
    isAnyMatchingEntries: KnockoutComputed<boolean>;
    
    allCategoryNames: KnockoutComputed<Array<string>>;
    selectedCategory = ko.observable<string>();

    private categoriesGridController = ko.observable<virtualGridController<string>>();
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

    constructor() {
        super();
        
        this.bindToCurrentInstance("save", "refresh", "switchToEditMode", "exitEditMode", "setGroupToShow");
        this.initializeObservables();
    }

    private initializeObservables() {

        this.initializeDirtyFlag();
        
        this.isSaveEnabled = ko.pureComputed(() => {
            const isDirty = this.dirtyFlag().isDirty();
            return isDirty && !this.spinners.save();
        });

        this.allCategoryNames = ko.pureComputed(() => {
            return _.uniq(this.allEntries().map(entry => entry.data.Metadata.Category)).sort();
        });

        this.selectedCategory.subscribe(() => this.computeEntriesToShow());
        this.groupToShow.subscribe(() => this.computeEntriesToShow());
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

        const dropDownCondition = this.groupToShow() === "allEntries" ||
                                 (this.groupToShow() === "databaseEntries" && !entry.isServerWideOnlyEntry()) ||
                                 (this.groupToShow() === "customizedDatabaseEntries" && entry instanceof models.databaseEntry && entry.override());

        return categoryCondition && filterCondition && dropDownCondition;
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
                new hyperlinkColumn<string>(categoriesGrid, x => x, x => appUrl.forDatabaseSettings(this.activeDatabase()), "Category", "90%",
                    {
                        sortable: "string",
                        handler: (categoryToShow, event) => this.selectActionHandler(categoryToShow, event),
                        extraClassForLink: x => this.getCategoryStateClass(x)
                    })
            ]);
    }

    private fetchCategoriesData(): JQueryPromise<pagedResult<string>> {
        return $.when<pagedResult<string>>({
            items: this.allCategoryNames(),
            totalResultCount: this.allCategoryNames().length,
            resultEtag: null,
            additionalResultInfo: undefined
        })
    }

    private selectActionHandler(categoryToShow: string, event: JQueryEventObject) {
        event.preventDefault();
        this.setCategory(categoryToShow);
    }
    
    private setCategory(category: string) {
        this.selectedCategory(category);
        
        if (this.categoriesGridController()) {
            this.categoriesGridController().setSelectedItems([category]);
        }
    }

    private getCategoryStateClass(category: string) {
        let state: string;
        let errorState: string;

        this.allEntries().filter(entry => entry.data.Metadata.Category === category).forEach(entry => {
            const entryState = this.getEntryStateClass(entry);
            
            if (entryState === "customized-item-with-error") {
                errorState = entryState;
            }
    
            if (entryState === "customized-item") {
                state = entryState;
            } 
        });
        
        return errorState || state || ""; 
    }
    
    private getEntryStateClass(entry: models.settingsEntry) {
        let stateClass: string;
        
        if (entry instanceof models.databaseEntry && entry.override()) {
            stateClass = this.isValid(entry.validationGroup) ? "customized-item" : "customized-item-with-error";
        }
        
        return stateClass || "";
    }
    
    private initSummaryGrid() {
        const summaryGrid = this.summaryGridController();
        summaryGrid.headerVisible(true);

        summaryGrid.init(() => this.fetchSummaryData(), () =>
            [
                new textColumn<models.settingsEntry>(summaryGrid, x => x.keyName(), "Configuration Key", "30%", {
                    sortable: "string",
                    extraClass: x => this.getEntryStateClass(x)
                }),
                new textColumn<models.settingsEntry>(summaryGrid, x => x.effectiveValue(), "Effective Value", "40%", {
                    sortable: "string",
                    extraClass: (x) => x.effectiveValue() ? "effective-value" : ""
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
            ]);
    }
    
    private fetchSummaryData(): JQueryPromise<pagedResult<models.settingsEntry>> {
        const entriesForSummaryMode = _.filter(this.allEntries(), x => x.showEntry());
        
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

    private isEntriesValid() {
        let valid = true;
        let firstError = true;

        this.allEntries().map(entry => {
            if (this.getEntryStateClass(entry) === "customized-item-with-error") {
                valid = false;

                if (firstError) {
                    firstError = false;
                    this.setCategory(entry.data.Metadata.Category);
                }
            }
        });

        return valid;
    }
    
    private findFirstCategoryWithError() {
        let categoryWithError: string;
        
        this.allEntries().some(entry => {
            if (this.getEntryStateClass(entry) === "customized-item-with-error") {
                return categoryWithError = entry.data.Metadata.Category;
            }
        });
        
        return categoryWithError;
    }
    
    save() {
        const categoryWithErrors = this.findFirstCategoryWithError();
        if (categoryWithErrors) {
            this.setCategory(categoryWithErrors);
            
            if (this.viewMode() === "summaryMode") {
                this.switchToEditMode();
            }

            return;
        }

        const settingsToSave = this.allEntries().map(entry => {
            if (entry instanceof models.databaseEntry) {
                return entry.getEntrySetting();
            }
        }).filter(x => !!x);

        const settingsToSaveSorted = _.sortBy(settingsToSave, x => x.key);
        
        const saveSettingsModel = new saveDatabaseSettingsConfirm((settingsToSaveSorted));

        saveSettingsModel.result.done(result => {
            if (result.can) {
                this.spinners.save(true);

                new saveDatabaseSettingsCommand(this.activeDatabase(), settingsToSaveSorted)
                    .execute()
                    .done(() => {
                        this.fetchDatabaseSettings(this.activeDatabase())
                            .done(() => this.processAfterFetch());
                    })
                    .always(() => this.spinners.save(false))
                }
            });

        app.showBootstrapDialog(saveSettingsModel);
    }

    refresh() {
        if (this.dirtyFlag().isDirty()) {
            this.confirmRefresh()
                .then(result => {
                      if (result.can) {
                          this.executeRefresh();
                      } 
                });
        } else {
            this.executeRefresh();
        }
    }

    confirmRefresh() {
        const text = `<div><span class='bg-warning text-warning padding padding-xs'><i class='icon-warning margin-right margin-right-sm'></i>You have unsaved changes !</span></div>
                      <div>Clicking Refresh will reload all database settings from the server.</div>`;

        return this.confirmationMessage("Are you sure?", text,
            {
                html: true,
                buttons: ["Cancel", "Refresh"]
            });
    }
    
    executeRefresh() {
        this.spinners.refresh(true);

        this.fetchDatabaseSettings(this.activeDatabase(), true)
            .done(() => {
                this.processAfterFetch();
                this.dirtyFlag().reset();
            })
            .always(() => this.spinners.refresh(false));
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
        
        if (this.viewMode() === "editMode") {
            this.setupEditMode();
        } else {
            this.computeEntriesToShow();
        }
    }
    
    setGroupToShow(groupType: entriesGroupType) {
        this.groupToShow(groupType);
    }

    exitEditMode() {
        this.viewMode("summaryMode");
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
        if (reset) {
            this.categoriesGridController().reset(false);
        }
        
        this.setCategory(this.selectedCategory() || this.allCategoryNames()[0]);
        this.computeEntriesToShow();
       
        this.allEntries().forEach(entry => {
            entry.showEntry(this.shouldShowEntry(entry));

            if (entry instanceof models.databaseEntry) {
                entry.override.subscribe(() => {
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
}

export = databaseSettings;
