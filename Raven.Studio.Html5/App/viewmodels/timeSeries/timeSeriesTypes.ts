import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import changeSubscription = require("common/changeSubscription");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import timeSeries = require("models/timeSeries/timeSeries");
import editPointDialog = require("viewmodels/timeSeries/editPointDialog");
import timeSeriesType = require("models/timeSeries/timeSeriesType");
import pointChange = require("models/timeSeries/pointChange");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");
import getTypesCommand = require("commands/timeSeries/getTypesCommand");
import putPointCommand = require("commands/timeSeries/putPointCommand");
import putTypeCommand = require("commands/timeSeries/putTypeCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class timeSeriesTypes extends viewModelBase {

    viewType = viewType.TimeSeries;
    types = ko.observableArray<timeSeriesType>();
    selectedType = ko.observable<timeSeriesType>().subscribeTo("ActivateType").distinctUntilChanged();
    currentType = ko.observable<timeSeriesType>();
    typeToSelect: string;
    currentTypePagedItems = ko.observable<pagedList>();
    selectedKeysIndices = ko.observableArray<number>();
    selectedKeysText: KnockoutComputed<string>;
    hasKeys: KnockoutComputed<boolean>;
    hasAnyKeysSelected: KnockoutComputed<boolean>;
    hasAllKeysSelected: KnockoutComputed<boolean>;
    isAnyKeysAutoSelected = ko.observable<boolean>(false);
    isAllKeysAutoSelected = ko.observable<boolean>(false);
    keysSelection: KnockoutComputed<checkbox>;

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#keysGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = timeSeriesTypes.isInitialized;

    constructor() {
        super();

        this.selectedType.subscribe(t => this.selectedTypeChanged(t));

        this.hasKeys = ko.computed(() => {
            var selectedType: timeSeriesType = this.selectedType();
            if (!!selectedType) {
                return this.selectedType().keysCount() > 0;
            }
            return false;
        });

        this.hasAnyKeysSelected = ko.computed(() => this.selectedKeysIndices().length > 0);

        this.hasAllKeysSelected = ko.computed(() => {
            var numOfSelectedKeys = this.selectedKeysIndices().length;
            if (!!this.selectedType() && numOfSelectedKeys !== 0) {
                return numOfSelectedKeys === this.selectedType().keysCount();
            }
            return false;
        });

        this.selectedKeysText = ko.computed(() => {
            if (!!this.selectedKeysIndices()) {
                var documentsText = "time series";
                return documentsText;
            }
            return "";
        });

        this.keysSelection = ko.computed(() => {
            if (this.hasAllKeysSelected())
                return checkbox.Checked;
            if (this.hasAnyKeysSelected())
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
            this.typeToSelect = args.type;
        }

        var ts = this.activeTimeSeries();
        this.fetchTypes(ts).done(results => {
            this.typesLoaded(results, ts);
            timeSeriesTypes.isInitialized(true);
        });
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
        timeSeriesTypes.isInitialized(false);
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
        //    ko.postbox.subscribe("ChangePointValue", () => this.changePoint()),
            ko.postbox.subscribe("ChangesApiReconnected", (ts: timeSeries) => this.reloadTimeSeriesData(ts)),
            ko.postbox.subscribe("SortTypes", () => this.sortTypes())
        ];
    }

    private fetchTypes(ts: timeSeries): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getTypesCommand(ts).execute().done((results: timeSeriesType[]) => deferred.resolve(results));
        return deferred;
    }

    typesLoaded(types: Array<timeSeriesType>, ts: timeSeries) {
        this.types(types);

        if (this.typeToSelect)
            this.types.first(g => g.name === this.typeToSelect).activate();
        else {
            var allTypes = this.types();
            if (allTypes.length > 0) {
                allTypes[0].activate();
            }
        }
    }

    newPoint() {
        var type = this.currentType();
        var changeVm = new editPointDialog(new pointChange(new timeSeriesPoint(type.name, type.fields, "", moment().format(), type.fields.map(x => 0)), true), true);
        changeVm.updateTask.done((change: pointChange) => {
            new putPointCommand(change.type(), change.key(), change.at(), change.values(), this.activeTimeSeries())
                .execute()
                .done(() => this.refresh());
        });
        app.showDialog(changeVm);
    }

    refresh() {
        var selectedKeyName = this.selectedType().name;
        this.refreshGridAndKey(selectedKeyName);
    }

    refreshGridAndKey(changedKeyName: string) {
        var type = this.selectedType();
        if (type.name === changedKeyName) {
            this.getKeysGrid().refreshCollectionData();
        }
        type.getKeys().invalidateCache();
        this.selectNone();
    }

    private selectedTypeChanged(selected: timeSeriesType) {
        if (!!selected) {
            var pagedList = selected.getKeys();
            this.currentTypePagedItems(pagedList);
            this.currentType(selected);
        }
    }

    toggleSelectAll() {
        var pointsGrid = this.getKeysGrid();
        if (!!pointsGrid) {
            if (this.hasAnyKeysSelected()) {
                pointsGrid.selectNone();
            } else {
                pointsGrid.selectSome();
                this.isAnyKeysAutoSelected(this.hasAllKeysSelected() === false);
            }
        }
    }

    selectAll() {
        var typesGrid = this.getKeysGrid();
        var type: timeSeriesType = this.selectedType();
        if (!!typesGrid && !!type) {
            typesGrid.selectAll(type.keysCount());
        }
    }

    selectNone() {
        var pointsGrid = this.getKeysGrid();
        if (!!pointsGrid) {
            pointsGrid.selectNone();
        }
    }

    deleteSelectedKeys() {
        if (this.hasAllKeysSelected()) {
            // TODO: Deleting all keys is not supported currently
        } else {
            var grid = this.getKeysGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    private updateTypes(receivedTypes: Array<timeSeriesType>) {
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
                foundType.keysCount(receivedGroup.keysCount());
            }
        });
    }

    private refreshKeysData() {
        this.types().forEach((key: timeSeriesType) => {
            var pagedList = key.getKeys();
            pagedList.invalidateCache();
        });
    }

    private refreshKeys(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var ts = this.activeTimeSeries();

        this.fetchTypes(ts).done(results => {
            this.updateTypes(results);
            this.refreshKeysData();
            deferred.resolve();
        });

        return deferred;
    }

    private reloadTimeSeriesData(ts: timeSeries) {
        if (ts.name === this.activeTimeSeries().name) {
            this.refreshKeys().done(() => this.refreshKeysData());
        }
    }

    selectType(type: timeSeriesType, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            type.activate();
            var timeSeriesTypeUrl = appUrl.forTimeSeriesType(type.name, this.activeTimeSeries());
            router.navigate(timeSeriesTypeUrl, false);
        }
    }

    private getKeysGrid(): virtualTable {
        var gridContents = $(timeSeriesTypes.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    private sortTypes() {
        this.types.sort((c1: timeSeriesType, c2: timeSeriesType) => {
            return c1.name.toLowerCase() > c2.name.toLowerCase() ? 1 : -1;
        });
    }

    // Animation callbacks for the types list
    showTypeElement(element) {
        if (element.nodeType === 1 && timeSeriesTypes.isInitialized()) {
            $(element).hide().slideDown(500, () => {
                ko.postbox.publish("SortTypes");
                $(element).highlight();
            });
        }
    }

    hideTypeElement(element) {
        if (element.nodeType === 1) {
            $(element).slideUp(1000, () => { $(element).remove(); });
        }
    }
}

export = timeSeriesTypes;
