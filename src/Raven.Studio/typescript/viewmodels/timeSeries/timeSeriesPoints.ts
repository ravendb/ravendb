import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import changeSubscription = require("common/changeSubscription");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesType = require("models/timeSeries/timeSeriesType");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import getKeyCommand = require("commands/timeSeries/getKeyCommand");
import putPointCommand = require("commands/timeSeries/putPointCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import editPointDialog = require("viewmodels/timeSeries/editPointDialog");
import deleteKey = require("viewmodels/timeSeries/deleteKey");
import pointChange = require("models/timeSeries/pointChange");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");
import pagedResultSet = require("common/pagedResultSet");
import getPointsCommand = require("commands/timeSeries/getPointsCommand");

class timeSeriesPoints extends viewModelBase {

    viewType = viewType.TimeSeries;
    type = ko.observable<string>();
    fields = ko.observableArray<string>();
    key = ko.observable<string>();
    pointsCount = ko.observable<number>();
    minPoint = ko.observable<string>();
    maxPoint = ko.observable<string>();
    isFiltered = ko.observable<boolean>(false);
    startPointFilter = ko.observable<string>();
    endPointFilter = ko.observable<string>();
    isAggregated = ko.observable<boolean>(false);
    duration = ko.observable<string>();
    durationType = ko.observable<string>();

    pointsList = ko.observable<pagedList>();
    selectedPointsIndices = ko.observableArray<number>();
    selectedPointsText: KnockoutComputed<string>;
    hasPoints: KnockoutComputed<boolean>;
    hasAnyPointsSelected: KnockoutComputed<boolean>;
    hasAllPointsSelected: KnockoutComputed<boolean>;
    isAnyPointsAutoSelected = ko.observable<boolean>(false);
    isAllPointsAutoSelected = ko.observable<boolean>(false);
    pointsSelection: KnockoutComputed<checkbox>;

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#pointsGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = timeSeriesPoints.isInitialized;

    constructor() {
        super();

        this.hasPoints = ko.computed(() => {
            return this.pointsCount() > 0;
        });

        this.hasAnyPointsSelected = ko.computed(() => this.selectedPointsIndices().length > 0);

        this.hasAllPointsSelected = ko.computed(() => {
            var numOfSelectedPoints = this.selectedPointsIndices().length;
            if (!!this.key() && numOfSelectedPoints !== 0) {
                return numOfSelectedPoints === this.pointsCount();
            }
            return false;
        });

        this.selectedPointsText = ko.computed(() => {
            if (!!this.selectedPointsIndices()) {
                var documentsText = "points";
                return documentsText;
            }
            return "";
        });

        this.pointsSelection = ko.computed(() => {
            if (this.hasAllPointsSelected())
                return checkbox.Checked;
            if (this.hasAnyPointsSelected())
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });

        this.isFiltered.subscribe(() => {
            this.refresh();
        });
        this.startPointFilter.subscribe(() => {
            this.refresh();
        });
        this.endPointFilter.subscribe(() => {
            this.refresh();
        });

        this.isAggregated.subscribe(() => {
            this.refresh();
        });
        this.duration.subscribe(() => {
            this.refresh();
        });
        this.durationType.subscribe(() => {
            this.refresh();
        });
    }

    activate(args: any) {
        super.activate(args);

        //TODO: update this in documentation
        //this.updateHelpLink('G8CDCP');
    
        // We can optionally pass in a key name to view's URL, e.g. #timeSeries/timeSeries?key=Foo&timeSeriestorage=test
        if (args && args.type && args.key) {
            this.fetchKey(args.type, args.key).done((result: timeSeriesKeySummaryDto) => {
                this.type(result.Type.Type);
                this.fields(result.Type.Fields);
                this.key(result.Key);
                this.pointsCount(result.PointsCount);
                this.minPoint(result.MinPoint);
                this.startPointFilter(result.MinPoint);
                this.maxPoint(result.MaxPoint);
                this.endPointFilter(result.MaxPoint);
                if (!this.pointsList()) {
                    this.pointsList(this.createPointsPagedList());
                }
                timeSeriesPoints.isInitialized(true);
            });
        }
    }

    private createPointsPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchPoints(skip, take,
            this.isFiltered() ? this.startPointFilter() : null,
            this.isFiltered() ? this.endPointFilter() : null);
        var list = new pagedList(fetcher);
        list.collectionName = this.key();
        return list;
    }

    private fetchPoints(skip: number, take: number, start: string, end: string): JQueryPromise<pagedResultSet<timeSeriesPoint>> {
        var doneTask = $.Deferred<pagedResultSet<timeSeriesPoint>>();
        new getPointsCommand(this.activeTimeSeries(), skip, take, this.type(), this.fields(), this.key(), start, end).execute()
            .done((points: timeSeriesPoint[]) => doneTask.resolve(new pagedResultSet(points, this.isFiltered() ? this.pointsList().itemCount() + points.length : this.pointsCount())))
            .fail(xhr => doneTask.reject(xhr));
        return doneTask;
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
        timeSeriesPoints.isInitialized(false);
    }

    createNotifications(): Array<changeSubscription> {
        return [
            /*TODO: Implement this in changes api:
            changesContext.currentResourceChangesApi().watchAllTimeSeries((e: timeSeriesChangeNotification) => this.refreshPoints()),
            changesContext.currentResourceChangesApi().watchTimeSeriesBulkOperation(() => this.refreshPoints())*/
        ];
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
        //    ko.postbox.subscribe("ChangePointValue", () => this.changePoint()),
//            ko.postbox.subscribe("ChangesApiReconnected", (ts: timeSeries) => this.reloadData(ts)),
        ];
    }

    private fetchKey(type: string, key: string): JQueryPromise<any> {
        var deferred = $.Deferred();
        //TODO: new getKeyCommand(this.activeTimeSeries(), type, key).execute().done((result: timeSeriesKeySummaryDto) => deferred.resolve(result));
        return deferred;
    }

    newPoint() {
        var changeVm = new editPointDialog(new pointChange(new timeSeriesPoint(this.type(), this.fields(), this.key(), moment().format(), this.fields().map(x => 0)), true), true);
        changeVm.updateTask.done((change: pointChange) => {
            new putPointCommand(change.type(), change.key(), change.at(), change.values(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showDialog(changeVm);
    }

    refresh() {
        var grid = this.getPointsGrid();
        if (grid) {
            grid.refreshCollectionData();
        }
        var pointsList = this.pointsList();
        if (pointsList) {
            pointsList.invalidateCache();
        }
        this.selectNone();
    }

    changePoint() {
        var grid = this.getPointsGrid();
        if (!grid)
            return;

        var selectedPoint = <timeSeriesPoint>grid.getSelectedItems(1).first();
        var change = new pointChange(selectedPoint);
        var pointChangeVM = new editPointDialog(change, false);
        pointChangeVM.updateTask.done((change: pointChange, isNew: boolean) => {
            new putPointCommand(change.type(), change.key(), change.at(), change.values(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showDialog(pointChangeVM);
    }

    toggleSelectAll() {
        var pointsGrid = this.getPointsGrid();
        if (!!pointsGrid) {
            if (this.hasAnyPointsSelected()) {
                pointsGrid.selectNone();
            } else {
                pointsGrid.selectSome();
                this.isAnyPointsAutoSelected(this.hasAllPointsSelected() === false);
            }
        }
    }

    selectAll() {
        var pointsGrid = this.getPointsGrid();
        if (!!pointsGrid && !!this.key()) {
            pointsGrid.selectAll(this.pointsCount());
        }
    }

    selectNone() {
        var pointsGrid = this.getPointsGrid();
        if (!!pointsGrid) {
            pointsGrid.selectNone();
        }
    }

    deleteSelectedPoints() {
        if (this.hasAllPointsSelected()) {
            this.deleteKey();
        } else {
            var grid = this.getPointsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    private deleteKey() {
        var deleteKeyVm = new deleteKey(this.type(), this.key(), this.activeTimeSeries());
        deleteKeyVm.deletionTask.done(() => {
            this.navigate(appUrl.forTimeSeriesType(this.type(), this.activeTimeSeries()));
        });
        app.showDialog(deleteKeyVm);
    }

    selectType(type: timeSeriesType, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            type.activate();
            var timeSeriesTypeUrl = appUrl.forTimeSeriesType(type.name, this.activeTimeSeries());
            router.navigate(timeSeriesTypeUrl, false);
        }
    }

    private getPointsGrid(): virtualTable {
        var gridContents = $(timeSeriesPoints.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = timeSeriesPoints;
