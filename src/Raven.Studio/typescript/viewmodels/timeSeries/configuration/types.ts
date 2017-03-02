import app = require("durandal/app");
import router = require("plugins/router");
import changeSubscription = require("common/changeSubscription");
import appUrl = require("common/appUrl");
import timeSeries = require("models/timeSeries/timeSeries");
import putTypeCommand = require("commands/timeSeries/putTypeCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import editTypeDialog = require("viewmodels/timeSeries/editTypeDialog");
import timeSeriesType = require("models/timeSeries/timeSeriesType");
import getTypesCommand = require("commands/timeSeries/getTypesCommand");
import typeChange = require("models/timeSeries/typeChange");

class types extends viewModelBase {

    viewType = viewType.TimeSeries;
    typesList = ko.observable<any>(); //TODO: use type
    hasTypes: KnockoutComputed<boolean>;
    selectedTypesIndices = ko.observableArray<number>();
    hasAnyTypesSelected: KnockoutComputed<boolean>;
    isAnyTypesAutoSelected = ko.observable<boolean>(false);
    isAllTypesAutoSelected = ko.observable<boolean>(false);
    canChangeType: KnockoutComputed<boolean>;

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#typesGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = types.isInitialized;

    constructor() {
        super();

        this.hasTypes = ko.computed(() => {
            var typesList = this.typesList();
            return typesList && typesList.itemCount() > 0;
        });

       this.hasAnyTypesSelected = ko.computed(() => this.selectedTypesIndices().length > 0);
        this.canChangeType = ko.computed(() => {
            if (this.selectedTypesIndices().length !== 1)
                return false;
            var selectedItem = <timeSeriesType>this.getTypesGrid().getSelectedItems()[0];
            return selectedItem.keysCount() === 0;
        });
    }

    activate(args: any) {
        super.activate(args);

        //TODO: this.typesList(this.createTypesPagedList());
    }

    /* TODO
    private createTypesPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchTypes(skip, take);
        var list = new pagedList(fetcher);
        return list;
    }*/

    private fetchTypes(skip: number, take: number): JQueryPromise<pagedResult<any>> {
        var deffered = $.Deferred<pagedResult<any>>();
        new getTypesCommand(this.activeTimeSeries()).execute()
            .done((types: timeSeriesType[]) => {
                deffered.resolve({
                    items: types,
                    totalResultCount: 1024
                });
            });
        return deffered;
    }

    attached() {
        super.attached();

        /*super.createKeyboardShortcut("F2", () => this.editSelectedTimeSeries(), timeSeries.gridSelector);*/

        // Q. Why do we have to setup the grid shortcuts here, when the grid already catches these shortcuts?
        // A. Because if the focus isn't on the grid, but on the docs page itself, we still need to catch the shortcuts.
        /*var docsPageSelector = ".documents-page";
        this.createKeyboardShortcut("DELETE", () => this.getDocumentsGrid().deleteSelectedItems(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, D", () => this.copySelectedDocs(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, I", () => this.copySelectedDocIds(), docsPageSelector);*/
    }

    deactivate() {
        super.deactivate();
        types.isInitialized(false);
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.resourceChangesApi();
        //TODO: this.addNotification(changesApi.watchAllTimeSeries((e: timeSeriesChangeNotification) => this.refreshTypes()));
        //TODO: this.addNotification(changesApi.watchTimeSeriesBulkOperation(() => this.refreshTypes()));
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            //    ko.postbox.subscribe("ChangeTypeValue", () => this.changeType()),
            //            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (ts: timeSeries) => this.reloadData(ts)),
        ];
    }

    newType() {
        var changeVm = new editTypeDialog(new typeChange(new timeSeriesType("", [""], 0, this.activeTimeSeries()), true), true);
        changeVm.updateTask.done((change: typeChange) => {
            new putTypeCommand(change.type(), change.fields(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showBootstrapDialog(changeVm);
    }

    changeType() {
        var grid = this.getTypesGrid();
        if (!grid)
            return;

        var selectedType = <timeSeriesType>grid.getSelectedItems(1)[0];
        var change = new typeChange(selectedType);
        var typeChangeVM = new editTypeDialog(change, false);
        typeChangeVM.updateTask.done((change: typeChange, isNew: boolean) => {
            new putTypeCommand(change.type(), change.fields(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showBootstrapDialog(typeChangeVM);
    }

    refresh() {
        this.getTypesGrid().refreshCollectionData();
        this.typesList().invalidateCache();
    }

    deleteSelectedTypes() {
        var grid = this.getTypesGrid();
        if (grid) {
            grid.deleteSelectedItems();
        }
    }

    selectType(type: timeSeriesType, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            type.activate();
            var timeSeriesTypeUrl = appUrl.forTimeSeriesType(type.name, this.activeTimeSeries());
            router.navigate(timeSeriesTypeUrl, false);
        }
    }

    private getTypesGrid() {
        var gridContents = $(types.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = types;
