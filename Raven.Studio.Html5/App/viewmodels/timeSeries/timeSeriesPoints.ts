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
import pointChange = require("models/timeSeries/pointChange");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class timeSeriesPoints extends viewModelBase {

    type: string;
    key: string;
    types = ko.observableArray<timeSeriesType>([]);
    currentKey = ko.observable<timeSeriesKey>();

    pagedItems = ko.observable<pagedList>();
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
            var currentKey= this.currentKey();
            if (!currentKey)
                return false;
            return currentKey.Points > 0;
        });

        this.hasAnyPointsSelected = ko.computed(() => this.selectedPointsIndices().length > 0);

        this.hasAllPointsSelected = ko.computed(() => {
            var numOfSelectedPoints = this.selectedPointsIndices().length;
            var currentKey = this.currentKey();
            if (!!currentKey && numOfSelectedPoints !== 0) {
                return numOfSelectedPoints === currentKey.Points;
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
    }

    activate(args) {
        super.activate(args);

        //TODO: update this in documentation
        //this.updateHelpLink('G8CDCP');

        // We can optionally pass in a key name to view's URL, e.g. #timeSeries/timeSeries?key=Foo&timeSeriestorage=test
        if (args && args.type && args.key) {
            this.type = args.type;
            this.key = args.key;

            this.fetchKey().done(result => {
                this.currentKey(result);
                this.pagedItems(this.currentKey().getPoints());
                timeSeriesPoints.isInitialized(true);
            });
        }
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

    private fetchKey(): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getKeyCommand(this.activeTimeSeries(), this.type, this.key).execute().done((result: timeSeriesKey) => deferred.resolve(result));
        return deferred;
    }

    newPoint() {
        var key = this.currentKey();
        var fields = key.Fields;
        var changeVm = new editPointDialog(new pointChange(new timeSeriesPoint("", fields, "", moment().format(), fields.map(x => 0)), true), true);
        changeVm.updateTask.done((change: pointChange) => {
            new putPointCommand(change.type(), change.key(), change.at(), change.values(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showDialog(changeVm);
    }

    refresh() {
        this.getPointsGrid().refreshCollectionData();
        this.currentKey().getPoints().invalidateCache();
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
        var currentKey = this.currentKey();
        if (!!pointsGrid && !!currentKey) {
            pointsGrid.selectAll(currentKey.Points);
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
            this.deleteKey(this.currentKey());
        } else {
            var grid = this.getPointsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    private deleteKey(key: timeSeriesKey) {
	  /*  var deleteGroupVm = new deleteGroup(key, this.activeTimeSeries());
            deleteGroupVm.deletionTask.done(() => {
				if (!key.isAllPointsGroup) {
                    this.points.remove(key);

                    var selectedCollection: timeSeriesType = this.selectedPoint();
                    if (key.name === selectedCollection.name) {
                        this.selectedPoint(this.allPointsGroup);
                    }
                } else {
                    this.refreshGridAndGroup(key.name);
                }
            });
		app.showDialog(deleteGroupVm);*/
    }

    /*private updateTypes(receivedTypes: Array<timeSeriesType>) {
        var deletedTypes = [];

        this.types().forEach((t: timeSeriesType) => {
            if (!receivedTypes.first((receivedGroup: timeSeriesType) => t.name === receivedGroup.name)) {
                deletedTypes.push(t);
            }
        });

        this.types.removeAll(deletedTypes);
        receivedTypes.forEach((receivedGroup: timeSeriesType) => {
            var foundType = this.types().first((t: timeSeriesType) => t.name === receivedGroup.name);
            if (!foundType) {
                this.types.push(receivedGroup);
            } else {
                foundType.pointsCount(receivedGroup.pointsCount());
            }
        });
    }

    private refreshPointsData() {
        var selectedGroup: timeSeriesType = this.selectedType();

        this.types().forEach((key: timeSeriesType) => {
            if (key.name === selectedGroup.name) {
                var docsGrid = this.getPointsGrid();
                if (!!docsGrid) {
                    docsGrid.refreshCollectionData();
                }
            } else {
                var pagedList = key.getTimeSeries();
                pagedList.invalidateCache();
            }
        });
    }

    private refreshPoints(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var ts = this.activeTimeSeries();

        this.fetchPoints(ts).done(results => {
            this.updateTypes(results);
	        this.refreshPointsData();
            deferred.resolve();
        });

        return deferred;
    }*/
/*
    private reloadData(ts: timeSeries) {
        if (ts.name === this.activeTimeSeries().name) {
            this.refreshPoints().done(() => this.refreshPointsData());
        }
    }*/

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