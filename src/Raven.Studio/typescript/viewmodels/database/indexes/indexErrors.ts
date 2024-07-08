import app = require("durandal/app");
import awesomeMultiselect = require("common/awesomeMultiselect");
import generalUtils = require("common/generalUtils");
import clearIndexErrorsConfirm = require("viewmodels/database/indexes/clearIndexErrorsConfirm");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import getIndexesErrorCountCommand from "commands/database/index/getIndexesErrorCountCommand";
import indexErrorInfoModel from "models/database/index/indexErrorInfoModel";
import getIndexesErrorCommand from "commands/database/index/getIndexesErrorCommand";
import moment = require("moment");

type nameAndCount = {
    name: string;
    count: number;
}

class indexErrors extends shardViewModelBase {

    view = require("views/database/indexes/indexErrors.html");

    private errorInfoItems = ko.observableArray<indexErrorInfoModel>([]);
    
    private selectedIndexNames = ko.observableArray<string>([]);
    private selectedActionNames = ko.observableArray<string>([]);

    searchText = ko.observable<string>();
    allIndexesSelected: KnockoutComputed<boolean>;

    clearErrorsBtnText: KnockoutComputed<string>;
    hasErrors: KnockoutComputed<boolean>;
    
    private debouncedCriteriaChanged = _.debounce(() => this.onSearchCriteriaChanged(), 600);

    private erroredIndexNames: KnockoutComputed<nameAndCount[]>;
    private erroredActionNames: KnockoutComputed<nameAndCount[]>;

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("toggleDetails", "clearIndexErrorsForItem", "clearIndexErrorsForAllItems", "fetchErrorCount", "filterItems");

        this.initObservables();
    }

    private initObservables() {
        this.searchText.subscribe(() => this.debouncedCriteriaChanged());
        this.selectedIndexNames.subscribe(() => this.debouncedCriteriaChanged());
        this.selectedActionNames.subscribe(() => this.debouncedCriteriaChanged());

        this.clearErrorsBtnText = ko.pureComputed(() => this.getButtonText());

        this.hasErrors = ko.pureComputed(() => this.errorInfoItems().some(x => x.totalErrorCount() > 0));

        this.allIndexesSelected = ko.pureComputed(() => this.erroredIndexNames().length === this.selectedIndexNames().length);
        
        this.erroredIndexNames = ko.pureComputed<nameAndCount[]>(() => {
            const result = new Map<string, number>();
            
            const items = this.errorInfoItems();
            items.forEach(item => {
                item.indexErrorsCountDto().forEach(countDto => {
                    const prevCount = result.get(countDto.Name) || 0;
                    const currentCount = countDto.Errors.reduce((count, val) => val.NumberOfErrors + count, 0);
                    result.set(countDto.Name, prevCount + currentCount);
                });
            });
            
            const mappedResult = Array.from(result.entries()).map(([name, count]) => ({ name, count }));
            return _.sortBy(mappedResult, x => x.name.toLocaleLowerCase());
        });
        
        this.erroredActionNames = ko.pureComputed(() => {
            const result = new Map<string, number>();
            
            const items = this.errorInfoItems();
            items.forEach(item => {
                item.indexErrorsCountDto().forEach(countDto => {
                    countDto.Errors.forEach(error => {
                        const prevCount = result.get(error.Action) || 0;
                        const currentCount = error.NumberOfErrors;
                        result.set(error.Action, prevCount + currentCount);
                    })
                })
            })
            
            const mappedResult = Array.from(result.entries()).map(([name, count]) => ({ name, count }));
            return _.sortBy(mappedResult, x => x.name.toLocaleLowerCase());
        });
    }
    
    private getButtonText() {
        if (this.allIndexesSelected() && this.erroredIndexNames().length) {
            return "Clear errors (All indexes)";
        } else if (this.selectedIndexNames().length) {
            return "Clear errors (Selected indexes)";
        } else {
            return "Clear errors";
        }
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('ABUXGF');

        this.fetchAllErrorCount();
    }

    fetchAllErrorCount() {
        const locations = this.db.getLocations();
        const models = locations.map(location => new indexErrorInfoModel(this.db.name, location));
        this.errorInfoItems(models);
        
        models.forEach(this.fetchErrorCount);
    }

    fetchErrorCount(model: indexErrorInfoModel): JQueryPromise<any> {
        return new getIndexesErrorCountCommand(this.db, model.location)
            .execute()
            .done(results => {
                const errorsCountDto = results.Results;
                
                model.onCountsLoaded(errorsCountDto);
                
                this.selectedIndexNames(this.erroredIndexNames().map(x => x.name));
                this.selectedActionNames(this.erroredActionNames().map(x => x.name));

                indexErrors.syncMultiSelect();
            })
            .fail((result) => {
                model.onCountsLoadError(result.responseJSON.Message);
            });
    }

    attached() {
        super.attached();

        awesomeMultiselect.build($("#visibleIndexesSelector"), opts => {
            opts.enableHTML = true;
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " indexes selected";
            opts.allSelectedText = "All indexes selected";
            opts.maxHeight = 500;
            opts.optionLabel = (element: HTMLOptionElement) => {
                const indexName = $(element).text();
                const indexNameEscaped = generalUtils.escape(indexName);
                const indexItem = this.erroredIndexNames().find(x => x.name === indexName);
                const indexItemCount = indexItem.count.toLocaleString();
                return `<span class="name" title="${indexNameEscaped}">${indexNameEscaped}</span><span class="badge" title="${indexItemCount}">${indexItemCount}</span>`;
            };
        });

        awesomeMultiselect.build($("#visibleActionsSelector"), opts => {
            opts.enableHTML = true;
            opts.includeSelectAllOption = true;
            opts.nSelectedText = " actions selected";
            opts.allSelectedText = "All actions selected";
            opts.maxHeight = 500;
            opts.optionLabel = (element: HTMLOptionElement) => {
                const actionName = $(element).text();
                const actionNameEscaped = generalUtils.escape(actionName);
                const actionItem = this.erroredActionNames().find(x => x.name === actionName);
                const actionItemCount = actionItem.count.toLocaleString();
                return `<span class="name" title="${actionNameEscaped}">${actionNameEscaped}</span><span class="badge" title="${actionItemCount}">${actionItemCount}</span>`;
            };
        });
    }

    private static syncMultiSelect() {
        awesomeMultiselect.rebuild($("#visibleIndexesSelector"));
        awesomeMultiselect.rebuild($("#visibleActionsSelector"));
    }

    compositionComplete() {
        super.compositionComplete();
        indexErrors.syncMultiSelect();
    }

    refresh() {
        this.fetchAllErrorCount();
    }

    private onSearchCriteriaChanged() {
        this.errorInfoItems().forEach(item => {
            if (item.showDetails()) {
                item.filterAndShow(this.filterItems);
            }
        });
    }

    toggleDetails(item: indexErrorInfoModel) {
        if (!item.showDetails()) {
            // details are not visible - start fetching data
            this.fetchErrorsDetails(item);
            item.onDetailsLoading();
        }
        
        item.showDetails.toggle();
    }

    private fetchErrorsDetails(item: indexErrorInfoModel) {
        new getIndexesErrorCommand(this.db, item.location)
            .execute()
            .then((resultDto: Raven.Client.Documents.Indexes.IndexErrors[]) => {
                const results = this.mapItems(resultDto);
                item.onDetailsLoaded(results);
                item.filterAndShow(this.filterItems);
            })
            .fail((result: JQueryXHR) => item.onDetailsLoadError(result.responseJSON.Message));
    }

    private mapItems(indexErrorsDto: Raven.Client.Documents.Indexes.IndexErrors[]): IndexErrorPerDocument[] {
        const mappedItems = _.flatMap(indexErrorsDto, value => {
            return value.Errors.map((errorDto: Raven.Client.Documents.Indexes.IndexingError): IndexErrorPerDocument =>
                ({
                    ...errorDto,
                    Timestamp: moment.utc(errorDto.Timestamp).format(),
                    IndexName: value.Name,
                    LocalTime: generalUtils.formatUtcDateAsLocal(errorDto.Timestamp),
                    RelativeTime: generalUtils.formatDurationByDate(moment.utc(errorDto.Timestamp), true)
                }));
        });

        return _.orderBy(mappedItems, [x => x.Timestamp], ["desc"]);
    }

    filterItems(error: IndexErrorPerDocument): boolean {
        if (!this.selectedIndexNames().includes(error.IndexName)) {
            return false;
        }
        if (!this.selectedActionNames().includes(error.Action)) {
            return false;
        }

        if (this.searchText()) {
            const searchText = this.searchText().toLowerCase();
            
            const matchesDocument = error.Document && error.Document.toLowerCase().includes(searchText);
            const matchesError = error.Error.toLowerCase().includes(searchText);
            return matchesDocument || matchesError;
        }
        
        return true;
    }

    clearIndexErrorsForAllItems() {
        const listOfLocations = this.errorInfoItems().map(x => x.location);
        this.handleClearRequest(listOfLocations);
    }

    clearIndexErrorsForItem(item: indexErrorInfoModel) {
        this.handleClearRequest([item.location]);
    }

    private handleClearRequest(locations: databaseLocationSpecifier[]) {
        const clearErrorsDialog = new clearIndexErrorsConfirm(this.allIndexesSelected() ? null : this.selectedIndexNames(), this.db, locations);
        app.showBootstrapDialog(clearErrorsDialog);

        clearErrorsDialog.clearErrorsTask
            .done((errorsCleared: boolean) => {
                if (errorsCleared) {
                    this.refresh();
                }
            });
    }
}

export = indexErrors; 
