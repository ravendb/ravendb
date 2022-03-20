import app = require("durandal/app");
import awesomeMultiselect = require("common/awesomeMultiselect");
import generalUtils = require("common/generalUtils");
import clearIndexErrorsConfirm = require("viewmodels/database/indexes/clearIndexErrorsConfirm");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import getIndexesErrorCountCommand from "commands/database/index/getIndexesErrorCountCommand";
import indexErrorInfoModel from "models/database/index/indexErrorInfoModel";
import getIndexesErrorCommand from "commands/database/index/getIndexesErrorCommand";

type nameAndCount = {
    name: string;
    count: number;
}

class indexErrors extends shardViewModelBase {

    view = require("views/database/indexes/indexErrors.html");

    private errorInfoItems = ko.observableArray<indexErrorInfoModel>([]);

    private erroredIndexNames = ko.observableArray<nameAndCount>([]);
    private selectedIndexNames = ko.observableArray<string>([]);

    private erroredActionNames = ko.observableArray<nameAndCount>([]);
    private selectedActionNames = ko.observableArray<string>([]);

    searchText = ko.observable<string>();
    allIndexesSelected: KnockoutComputed<boolean>;

    clearErrorsBtnText: KnockoutComputed<string>;
    hasErrors: KnockoutComputed<boolean>;

    constructor(db: database) {
        super(db);
        this.bindToCurrentInstance("toggleDetails", "clearIndexErrorsForItem", "clearIndexErrorsForAllItems");

        this.initObservables();
    }

    private initObservables() {
        this.searchText.throttle(500).subscribe(() => this.onSearchCriteriaChanged());
        this.selectedIndexNames.throttle(500).subscribe(() => this.onSearchCriteriaChanged());
        this.selectedActionNames.throttle(500).subscribe(() => this.onSearchCriteriaChanged());

        this.clearErrorsBtnText = ko.pureComputed(() => this.getButtonText());

        this.hasErrors = ko.pureComputed(() => this.errorInfoItems().some(x => x.totalErrorCount() > 0));

        this.allIndexesSelected = ko.pureComputed(() => this.erroredIndexNames().length === this.selectedIndexNames().length);
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
        this.erroredIndexNames([]);
        this.erroredActionNames([]);
        this.errorInfoItems([]);

        this.db.getLocations().map(location => this.fetchErrorCountForLocation(location));
    }

    private fetchErrorCountForLocation(location: databaseLocationSpecifier): JQueryPromise<any> {
        return new getIndexesErrorCountCommand(this.db, location)
            .execute()
            .done(results => {
                const errorsCountDto = results.Results;

                // calc model item
                const item = new indexErrorInfoModel(this.db.name, location, errorsCountDto);
                this.errorInfoItems.push(item);

                // calc index names for top dropdown
                errorsCountDto.forEach(resultItem => {
                    const index = this.erroredIndexNames().find(x => x.name === resultItem.Name);
                    const countToAdd = resultItem.Errors.reduce((count, val) => val.NumberOfErrors + count, 0);
                    
                    if (index) {
                        index.count += countToAdd;
                    } else {
                        const item: nameAndCount = {
                            name: resultItem.Name,
                            count: countToAdd
                        };
                        this.erroredIndexNames.push(item);
                    }
                });

                // calc actions for top dropdown
                errorsCountDto.forEach(resultItem => {
                    resultItem.Errors.forEach(errItem => {
                        const action = this.erroredActionNames().find(x => x.name === errItem.Action);
                        if (action) {
                            action.count += errItem.NumberOfErrors;
                        } else {
                            const item: nameAndCount = {
                                name: errItem.Action,
                                count: errItem.NumberOfErrors
                            }
                            this.erroredActionNames.push(item);
                        }
                    });
                });

                this.erroredIndexNames(_.sortBy(this.erroredIndexNames(), x => x.name.toLocaleLowerCase()));
                this.erroredActionNames(_.sortBy(this.erroredActionNames(), x => x.name.toLocaleLowerCase()));
                this.errorInfoItems(_.sortBy(this.errorInfoItems(), x => x.location().nodeTag))

                this.selectedIndexNames(this.erroredIndexNames().map(x => x.name));
                this.selectedActionNames(this.erroredActionNames().map(x => x.name));

                indexErrors.syncMultiSelect();
            })
            .fail((result) => {
                const item = new indexErrorInfoModel(this.db.name, location, null, result.responseJSON.Message);
                this.errorInfoItems.push(item);
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
                this.fetchErrorsDetails(item);
            }
        });
    }

    toggleDetails(item: indexErrorInfoModel) {
        item.showDetails.toggle();
        
        if (item.showDetails()) {
            this.fetchErrorsDetails(item);
        }
    }

    private fetchErrorsDetails(item: indexErrorInfoModel) {
        new getIndexesErrorCommand(this.db, item.location())
            .execute()
            .then((resultDto: Raven.Client.Documents.Indexes.IndexErrors[]) => {
                const results: IndexErrorPerDocument[] = this.mapItems(resultDto);
                const filteredResults: IndexErrorPerDocument[] = this.filterItems(results);
                item.filteredIndexErrors(filteredResults);
                item.errMsg("");
            })
            .fail(result => item.errMsg(result));
    }

    private mapItems(indexErrorsDto: Raven.Client.Documents.Indexes.IndexErrors[]): IndexErrorPerDocument[] {
        const mappedItems = _.flatMap(indexErrorsDto, value => {
            return value.Errors.map((errorDto: Raven.Client.Documents.Indexes.IndexingError): IndexErrorPerDocument =>
                ({
                    Timestamp: errorDto.Timestamp,
                    Document: errorDto.Document,
                    Action: errorDto.Action,
                    Error: errorDto.Error,
                    IndexName: value.Name
                }));
        });

        return _.orderBy(mappedItems, [x => x.Timestamp], ["desc"]);
    }

    private filterItems(list: IndexErrorPerDocument[]): IndexErrorPerDocument[] {

        let filteredItems = list.filter(error => this.selectedIndexNames().includes(error.IndexName) &&
            this.selectedActionNames().includes(error.Action));

        if (this.searchText()) {
            const searchText = this.searchText().toLowerCase();
            
            filteredItems = filteredItems.filter((error) =>
                (error.Document && error.Document.toLowerCase().includes(searchText)) ||
                error.Error.toLowerCase().includes(searchText))
        }

        return filteredItems;
    }

    clearIndexErrorsForAllItems() {
        const listOfLocations = this.errorInfoItems().map(x => x.location());
        this.handleClearRequest(listOfLocations);
    }

    clearIndexErrorsForItem(item: indexErrorInfoModel) {
        this.handleClearRequest([item.location()]);
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
