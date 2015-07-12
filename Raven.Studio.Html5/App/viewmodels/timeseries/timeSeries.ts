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

class timeSeries extends viewModelBase {

    keys = ko.observableArray<timeSeriesKey>();
    selectedKey = ko.observable<timeSeriesKey>().subscribeTo("ActivateKey").distinctUntilChanged();
    currentKey = ko.observable<timeSeriesKey>();
    keyToSelectName: string;
    currentKeyPagedItems = ko.observable<pagedList>();
    selectedTimeSeriesIndices = ko.observableArray<number>();
    selectedTimeSeriesText: KnockoutComputed<string>;
    hasTimeSeries: KnockoutComputed<boolean>;
    hasAnyTimeSeriesSelected: KnockoutComputed<boolean>;
    hasAllTimeSeriesSelected: KnockoutComputed<boolean>;
    isAnyTimeSeriesAutoSelected = ko.observable<boolean>(false);
    isAllTimeSeriesAutoSelected = ko.observable<boolean>(false);

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#timeSeriesGrid";
	static isInitialized = ko.observable<boolean>(false);
	isInitialized = timeSeries.isInitialized;

	constructor() {
        super();

        this.selectedKey.subscribe(c => this.selectedKeyChanged(c));

        this.hasTimeSeries = ko.computed(() => {
            var selectedKey: timeSeriesKey = this.selectedKey();
            if (!!selectedKey) {
                return this.selectedKey().timeSeriesCount() > 0;
            }
            return false;
        });

        this.hasAnyTimeSeriesSelected = ko.computed(() => this.selectedTimeSeriesIndices().length > 0);

        this.hasAllTimeSeriesSelected = ko.computed(() => {
            var numOfSelectedTimeSeries = this.selectedTimeSeriesIndices().length;
            if (!!this.selectedKey() && numOfSelectedTimeSeries !== 0) {
                return numOfSelectedTimeSeries === this.selectedKey().timeSeriesCount();
            }
            return false;
        });

        this.selectedTimeSeriesText = ko.computed(() => {
            if (!!this.selectedTimeSeriesIndices()) {
                var documentsText = "time series";
                return documentsText;
            }
            return "";
        });
    }

    activate(args) {
        super.activate(args);

        //TODO: update this in documentation
        //this.updateHelpLink('G8CDCP');

        // We can optionally pass in a key name to view's URL, e.g. #timeSeriestorages/timeSeries?key=Foo&timeSeriestorage=test
        this.keyToSelectName = args ? args.key : null;

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
            ko.postbox.subscribe("ChangeTimeSeriesPointValue", () => this.changePoint()),
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

        var keyToSelect = this.keys.first(g => g.name === this.keyToSelectName);
        if (keyToSelect) {
            keyToSelect.activate();
        }
    }

    newPoint() {/*
        var timeSeriesChangeVm = new editTimeSeriesDialog();
        timeSeriesChangeVm.updateTask.done((change: timeSeriesChange) => {
            var timeSeriesCommand = new updateTimeSeriesCommand(this.activeTimeSeries(), change.key(), change.timeSeriesName(), change.delta(), change.isNew());
            var execute = timeSeriesCommand.execute();
			execute.done(() => this.refreshGridAndGroup(change.key()));
        });
        app.showDialog(timeSeriesChangeVm);*/
    }

	refresh() {
		var selectedKeyName = this.selectedKey().name;
		this.refreshGridAndKey(selectedKeyName);
	}

    changePoint() {
        /*var grid = this.getTimeSeriesGrid();
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
			this.getTimeSeriesGrid().refreshCollectionData();
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
        var timeSeriesGrid = this.getTimeSeriesGrid();
        if (!!timeSeriesGrid) {
            if (this.hasAnyTimeSeriesSelected()) {
                timeSeriesGrid.selectNone();
            } else {
                timeSeriesGrid.selectSome();
                this.isAnyTimeSeriesAutoSelected(this.hasAllTimeSeriesSelected() === false);
            }
        }
    }

    selectAll() {
        var timeSeriesGrid = this.getTimeSeriesGrid();
        var key: timeSeriesKey = this.selectedKey();
        if (!!timeSeriesGrid && !!key) {
            timeSeriesGrid.selectAll(key.timeSeriesCount());
        }
    }

    selectNone() {
        var timeSeriesGrid = this.getTimeSeriesGrid();
        if (!!timeSeriesGrid) {
            timeSeriesGrid.selectNone();
        }
    }

    deleteSelectedTimeSeries() {
        if (this.hasAllTimeSeriesSelected()) {
            this.deleteGroupInternal(this.selectedKey());
        } else {
            var grid = this.getTimeSeriesGrid();
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
                foundGroup.timeSeriesCount(receivedGroup.timeSeriesCount());
            }
        });
    }

    private refreshKeysData() {
        var selectedGroup: timeSeriesKey = this.selectedKey();

        this.keys().forEach((key: timeSeriesKey) => {
            if (key.name === selectedGroup.name) {
                var docsGrid = this.getTimeSeriesGrid();
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

    private getTimeSeriesGrid(): virtualTable {
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