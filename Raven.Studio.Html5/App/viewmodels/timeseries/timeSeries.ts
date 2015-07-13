import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import changeSubscription = require("common/changeSubscription");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import timeSeriesDocument = require("models/timeSeries/timeSeriesDocument");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import getTimeSeriesKeysCommand = require("commands/timeSeries/getTimeSeriesKeysCommand");
import viewModelBase = require("viewmodels/viewModelBase");
// import editPointDialog = require("viewmodels/timeSeries/editPointDialog");

class timeSeries extends viewModelBase {

    keys = ko.observableArray<timeSeriesKey>();
    selectedKey = ko.observable<timeSeriesKey>().subscribeTo("ActivateKey").distinctUntilChanged();
    currentKey = ko.observable<timeSeriesKey>();
    keyAndPrefixToSelect: string;
    currentKeyPagedItems = ko.observable<pagedList>();
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
	isInitialized = timeSeries.isInitialized;

	constructor() {
        super();

        this.selectedKey.subscribe(c => this.selectedKeyChanged(c));

        this.hasPoints = ko.computed(() => {
            var selectedKey: timeSeriesKey = this.selectedKey();
            if (!!selectedKey) {
                return this.selectedKey().pointsCount() > 0;
            }
            return false;
        });

        this.hasAnyPointsSelected = ko.computed(() => this.selectedPointsIndices().length > 0);

        this.hasAllPointsSelected = ko.computed(() => {
            var numOfSelectedPoints = this.selectedPointsIndices().length;
            if (!!this.selectedKey() && numOfSelectedPoints !== 0) {
                return numOfSelectedPoints === this.selectedKey().pointsCount();
            }
            return false;
        });

        this.selectedPointsText = ko.computed(() => {
            if (!!this.selectedPointsIndices()) {
                var documentsText = "time series";
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
        if (args && args.prefix && args.key) {
            this.keyAndPrefixToSelect = args.prefix + "/" + args.key;
        }

        var ts = this.activeTimeSeries();
        this.fetchKeys(ts).done(results => {
	        this.keysLoaded(results, ts);
	        timeSeries.isInitialized(true);
        });
    }


    attached() {
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
		timeSeries.isInitialized(false);
	}

    createNotifications(): Array<changeSubscription> {
        return [
            /*TODO: Implement this in changes api:
            changesContext.currentResourceChangesApi().watchAllTimeSeries((e: timeSeriesChangeNotification) => this.refreshKeys()),
            changesContext.currentResourceChangesApi().watchTimeSeriesBulkOperation(() => this.refreshKeys())*/
        ];
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("ChangePointValue", () => this.changePoint()),
            ko.postbox.subscribe("ChangesApiReconnected", (ts: timeSeriesDocument) => this.reloadTimeSeriesData(ts)),
            ko.postbox.subscribe("SortKeys", () => this.sortKeys())
        ];
    }

    private fetchKeys(ts: timeSeriesDocument): JQueryPromise<any> {
        var deferred = $.Deferred();

        var getKeysCommand = new getTimeSeriesKeysCommand(ts);
        getKeysCommand.execute().done((results: timeSeriesKey[]) => deferred.resolve(results));
        return deferred;
    }

    keysLoaded(keys: Array<timeSeriesKey>, ts: timeSeriesDocument) {
        this.keys(keys);

        var keyToSelect = this.keyAndPrefixToSelect ? this.keys.first(g => g.name === this.keyAndPrefixToSelect) : this.keys()[0];
        keyToSelect.activate();
    }

    newPoint() {
        /*var changeVm = new editPointDialog();
        changeVm.updateTask.done((change: timeSeriesChange) => {
            var timeSeriesCommand = new updateTimeSeriesCommand(this.activeTimeSeries(), change.key(), change.timeSeriesName(), change.delta(), change.isNew());
            var execute = timeSeriesCommand.execute();
			execute.done(() => this.refreshGridAndGroup(change.key()));
        });
        app.showDialog(changeVm);*/
    }

	refresh() {
		var selectedKeyName = this.selectedKey().name;
		this.refreshGridAndKey(selectedKeyName);
	}

    changePoint() {
        /*var grid = this.getPointsGrid();
        if (grid) {
            var timeSeriesData = grid.getSelectedItems(1).first();
            var dto = {
                CurrentValue: timeSeriesData.Total,
                Group: timeSeriesData.Group,
                TimeSeriesName: timeSeriesData.Name,
                Delta: 0
            };
            var change = new timeSeriesChange(dto);
            var timeSeriesChangeVm = new editTimeSeriesDialog(change);
            timeSeriesChangeVm.updateTask.done((change: timeSeriesChange, isNew: boolean) => {
                var timeSeriesCommand = new updateTimeSeriesCommand(this.activeTimeSeries(), change.key(), change.timeSeriesName(), change.delta(), isNew);
	            var execute = timeSeriesCommand.execute();
				execute.done(() => this.refreshGridAndGroup(timeSeriesData.Group));
            });
            app.showDialog(timeSeriesChangeVm);
        }*/
    }

	refreshGridAndKey(changedKeyName: string) {
		var key = this.selectedKey();
		if (key.name === changedKeyName) {
			this.getPointsGrid().refreshCollectionData();
		}
		key.invalidateCache();
		this.selectNone();
	}

    private selectedKeyChanged(selected: timeSeriesKey) {
        if (!!selected) {
            var pagedList = selected.getTimeSeries();
            this.currentKeyPagedItems(pagedList);
            this.currentKey(selected);
        }
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
        var key: timeSeriesKey = this.selectedKey();
        if (!!pointsGrid && !!key) {
            pointsGrid.selectAll(key.pointsCount());
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
            this.deleteGroupInternal(this.selectedKey());
        } else {
            var grid = this.getPointsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    private deleteGroupInternal(key: timeSeriesKey) {
	  /*  var deleteGroupVm = new deleteGroup(key, this.activeTimeSeries());
            deleteGroupVm.deletionTask.done(() => {
				if (!key.isAllKeysGroup) {
                    this.keys.remove(key);

                    var selectedCollection: timeSeriesKey = this.selectedKey();
                    if (key.name === selectedCollection.name) {
                        this.selectedKey(this.allKeysGroup);
                    }
                } else {
                    this.refreshGridAndGroup(key.name);
                }
            });
		app.showDialog(deleteGroupVm);*/
    }

    private updateKeys(receivedKeys: Array<timeSeriesKey>) {
        var deletedKeys = [];

        this.keys().forEach((key: timeSeriesKey) => {
            if (!receivedKeys.first((receivedGroup: timeSeriesKey) => key.name === receivedGroup.name)) {
                deletedKeys.push(key);
            }
        });

        this.keys.removeAll(deletedKeys);
        receivedKeys.forEach((receivedGroup: timeSeriesKey) => {
            var foundGroup = this.keys().first((key: timeSeriesKey) => key.name === receivedGroup.name);
            if (!foundGroup) {
                this.keys.push(receivedGroup);
            } else {
                foundGroup.pointsCount(receivedGroup.pointsCount());
            }
        });
    }

    private refreshKeysData() {
        var selectedGroup: timeSeriesKey = this.selectedKey();

        this.keys().forEach((key: timeSeriesKey) => {
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

    private refreshKeys(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var ts = this.activeTimeSeries();

        this.fetchKeys(ts).done(results => {
            this.updateKeys(results);
	        this.refreshKeysData();
            deferred.resolve();
        });

        return deferred;
    }

    private reloadTimeSeriesData(ts: timeSeriesDocument) {
        if (ts.name === this.activeTimeSeries().name) {
            this.refreshKeys().done(() => this.refreshKeysData());
        }
    }

    selectKey(key: timeSeriesKey, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            key.activate();
            var timeSeriesKeyUrl = appUrl.forTimeSeriesKey(key.prefix, key.key, this.activeTimeSeries());
            router.navigate(timeSeriesKeyUrl, false);
        }
    }

    private getPointsGrid(): virtualTable {
        var gridContents = $(timeSeries.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

	private sortKeys() {
		this.keys.sort((c1: timeSeriesKey, c2: timeSeriesKey) => {
			return c1.name.toLowerCase() > c2.name.toLowerCase() ? 1 : -1;
		});
	}

	// Animation callbacks for the keys list
	showKeyElement(element) {
		if (element.nodeType === 1 && timeSeries.isInitialized()) {
			$(element).hide().slideDown(500, () => {
				ko.postbox.publish("SortKeys");
				$(element).highlight();
			});
		}
	}

	hideKeyElement(element) {
		if (element.nodeType === 1) {
			$(element).slideUp(1000, () => { $(element).remove(); });
		}
	}
}

export = timeSeries;