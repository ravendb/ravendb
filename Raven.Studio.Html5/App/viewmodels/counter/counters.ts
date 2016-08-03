import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import changesContext = require("common/changesContext");
import changeSubscription = require("common/changeSubscription");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import counterStorage = require("models/counter/counterStorage");
import counterChange = require("models/counter/counterChange");
import counterGroup = require("models/counter/counterGroup");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import updateCounterCommand = require("commands/counter/updateCounterCommand");
import resetCounterCommand = require("commands/counter/resetCounterCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import editCounterDialog = require("viewmodels/counter/editCounterDialog");
import deleteGroup = require("viewmodels/counter/deleteGroup");

class counters extends viewModelBase {

    viewType = viewType.Counters;
    groups = ko.observableArray<counterGroup>();
    allGroupsGroup: counterGroup;
    isAllGroupsGroup: KnockoutObservable<boolean>;
    selectedGroup = ko.observable<counterGroup>().subscribeTo("ActivateGroup").distinctUntilChanged();
    currentGroup = ko.observable<counterGroup>();
    groupToSelectName: string;
    currentGroupPagedItems = ko.observable<pagedList>();
    selectedCounterIndices = ko.observableArray<number>();
    selectedCountersText: KnockoutComputed<string>;
    hasCounters: KnockoutComputed<boolean>;
    hasAnyCountersSelected: KnockoutComputed<boolean>;
    hasAllCountersSelected: KnockoutComputed<boolean>;
    isAnyCountersAutoSelected = ko.observable<boolean>(false);
    isAllCountersAutoSelected = ko.observable<boolean>(false);
    countersSelection: KnockoutComputed<checkbox>;
    
    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#countersGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = counters.isInitialized;

    constructor() {
        super();

        this.selectedGroup.subscribe(c => this.selectedGroupChanged(c));

        this.isAllGroupsGroup = ko.computed(() => !!this.selectedGroup() ? this.selectedGroup().isAllGroupsGroup : true);

        this.hasCounters = ko.computed(() => {
            var selectedGroup: counterGroup = this.selectedGroup();
            if (!!selectedGroup) {
                if (selectedGroup.name === counterGroup.allGroupsGroupName) {
                    var cs: counterStorage = this.activeCounterStorage();
                    return !!cs.statistics() ? cs.statistics().countersCount() > 0 : false;
                }
                return this.selectedGroup().countersCount() > 0;
            }
            return false;
        });

        this.hasAnyCountersSelected = ko.computed(() => this.selectedCounterIndices().length > 0);

        this.hasAllCountersSelected = ko.computed(() => {
            var numOfSelectedCounters = this.selectedCounterIndices().length;
            if (!!this.selectedGroup() && numOfSelectedCounters !== 0) {
                return numOfSelectedCounters === this.selectedGroup().countersCount();
            }
            return false;
        });

        this.selectedCountersText = ko.computed(() => {
            if (!!this.selectedCounterIndices()) {
                var documentsText = "counter";
                if (this.selectedCounterIndices().length !== 1) {
                    documentsText += "s";
                }
                return documentsText;
            }
            return "";
        });
        
        this.countersSelection = ko.computed(() => {
            if (this.hasAllCountersSelected())
                return checkbox.Checked;
            if (this.hasAnyCountersSelected())
                return checkbox.SomeChecked;
            return checkbox.UnChecked;
        });
    }

    activate(args) {
        super.activate(args);

        //TODO: update this in documentation
        //this.updateHelpLink('G8CDCP');

        // We can optionally pass in a group name to view's URL, e.g. #counterstorages/counters?group=Foo&counterstorage=test
        this.groupToSelectName = args ? args.group : null;

        var cs = this.activeCounterStorage();
                
        //initial groups fetch
        this.fetchGroups(cs, 0, Number.MAX_VALUE).done(results => {
            this.groupsLoaded(results, cs);
            counters.isInitialized(true);
        });
    }


    attached() {
        super.attached();
        /*super.createKeyboardShortcut("F2", () => this.editSelectedCounter(), counters.gridSelector);*/

        // Q. Why do we have to setup the grid shortcuts here, when the grid already catches these shortcuts?
        // A. Because if the focus isn't on the grid, but on the docs page itself, we still need to catch the shortcuts.
        /*var docsPageSelector = ".documents-page";
        this.createKeyboardShortcut("DELETE", () => this.getDocumentsGrid().deleteSelectedItems(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, D", () => this.copySelectedDocs(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, I", () => this.copySelectedDocIds(), docsPageSelector);*/
    }

    deactivate() {
        super.deactivate();
        counters.isInitialized(false);
    }

    createNotifications(): Array<changeSubscription> {
        return [
            changesContext.currentResourceChangesApi().watchAllCounters((e: counterChangeNotification) => this.refreshGroups()),
            changesContext.currentResourceChangesApi().watchCounterBulkOperation(() => this.refreshGroups())
        ];
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("ChangeCounterValue", () => this.change()),
            ko.postbox.subscribe("ResetCounter", () => this.reset()),
            ko.postbox.subscribe("ChangesApiReconnected", (cs: counterStorage) => this.reloadCountersData(cs)),
            ko.postbox.subscribe("SortGroups", () => this.sortGroups()),
            ko.postbox.subscribe("SelectGroup", (groupName: string) => {
                var groupToSelect = this.groups.first(g => g.name === groupName);
                if (!!groupToSelect) {
                    this.selectGroupInternal(groupToSelect);
                }
            })
        ];
    }

    private fetchGroups(cs: counterStorage, skip: number, pageSize: number): JQueryPromise<any> {
        var deferred = $.Deferred();

        var getGroupsCommand = new getCounterGroupsCommand(cs, skip, pageSize);
        getGroupsCommand.execute().done((results: counterGroup[]) => deferred.resolve(results));
        return deferred;
    }

    groupsLoaded(groups: Array<counterGroup>, cs: counterStorage) {
        // Create the "All Groups" pseudo collection.
        this.allGroupsGroup = counterGroup.createAllGroupsCollection(cs);
        this.allGroupsGroup.countersCount = ko.computed(() => !!cs.statistics() ? cs.statistics().countersCount() : 0);

        // All systems a-go. Load them into the UI and select the first one.
        var allGroups = [this.allGroupsGroup].concat(groups);
        this.groups(allGroups);

        var groupToSelect = this.groups.first(g => g.name === this.groupToSelectName) || this.allGroupsGroup;
        groupToSelect.activate();
    }

    newCounter() {
        var counterChangeVm: editCounterDialog;
        var currentGroupName = this.selectedGroup().name;
        if (currentGroupName === counterGroup.allGroupsGroupName) {
            counterChangeVm = new editCounterDialog();
        } else {
            var dto = {
                CurrentValue: 0,
                Group: currentGroupName,
                CounterName: "",
                Delta: 0
            };
            var change = new counterChange(dto, true);
            counterChangeVm = new editCounterDialog(change);
        }
        
        counterChangeVm.updateTask.done((change: counterChange) => {
            var counterCommand = new updateCounterCommand(this.activeCounterStorage(), change.group(), change.counterName(), change.delta(), change.isNew());
            var execute = counterCommand.execute();
            execute.done(() => this.refreshGridAndGroup(change.group()));
        });
        app.showDialog(counterChangeVm);
    }

    refresh() {
        var selectedGroupName = this.selectedGroup().name;
        this.refreshGridAndGroup(selectedGroupName);
    }

    edit() {
        var grid = this.getCountersGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    change() {
        var grid = this.getCountersGrid();
        if (grid) {
            var counterData = grid.getSelectedItems(1).first();
            var dto = {
                CurrentValue: counterData.Total,
                Group: counterData["Group Name"],
                CounterName: counterData["Counter Name"],
                Delta: 0
            };
            var change = new counterChange(dto);
            var counterChangeVm = new editCounterDialog(change);
            counterChangeVm.updateTask.done((change: counterChange, isNew: boolean) => {
                var counterCommand = new updateCounterCommand(this.activeCounterStorage(), change.group(), change.counterName(), change.delta(), isNew);
                var execute = counterCommand.execute();
                execute.done(() => this.refreshGridAndGroup(counterData.GroupName));
            });
            app.showDialog(counterChangeVm);
        }
    }

    reset() {
        var grid = this.getCountersGrid();
        if (grid) {
            var counterData = grid.getSelectedItems(1).first();
            var confirmation = this.confirmationMessage("Reset Counter", "Are you sure that you want to reset the counter?");
            confirmation.done(() => {
                var resetCommand = new resetCounterCommand(this.activeCounterStorage(), counterData["Group Name"], counterData["Counter Name"]);
                var execute = resetCommand.execute();
                execute.done(() => this.refreshGridAndGroup(counterData.GroupName));
            });
        }
    }

    refreshGridAndGroup(changedGroupName: string) {
        var group = this.selectedGroup();
        if (group.name === changedGroupName || group.name === counterGroup.allGroupsGroupName) {
            this.getCountersGrid().refreshCollectionData();
        }
        group.invalidateCache();
        this.selectNone();
    }

    private selectedGroupChanged(selected: counterGroup) {
        if (!!selected) {
            var pagedList = selected.getCounters();
            this.currentGroupPagedItems(pagedList);
            this.currentGroup(selected);
        }
    }

    toggleSelectAll() {
        var countersGrid = this.getCountersGrid();
        if (!!countersGrid) {
            if (this.hasAnyCountersSelected()) {
                countersGrid.selectNone();
            } else {
                countersGrid.selectSome();
                this.isAnyCountersAutoSelected(this.hasAllCountersSelected() === false);
            }
        }
    }

    selectAll() {
        var countersGrid = this.getCountersGrid();
        var group: counterGroup = this.selectedGroup();
        if (!!countersGrid && !!group) {
            countersGrid.selectAll(group.countersCount());
        }
    }

    selectNone() {
        var countersGrid = this.getCountersGrid();
        if (!!countersGrid) {
            countersGrid.selectNone();
        }
    }

    deleteSelectedCounters() {
        if (this.hasAllCountersSelected()) {
            this.deleteGroupInternal(this.selectedGroup());
        } else {
            var grid = this.getCountersGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    private deleteGroupInternal(group: counterGroup) {
        var deleteGroupVm = new deleteGroup(group, this.activeCounterStorage());
            deleteGroupVm.deletionTask.done(() => {
                if (!group.isAllGroupsGroup) {
                    this.groups.remove(group);

                    var selectedCollection: counterGroup = this.selectedGroup();
                    if (group.name === selectedCollection.name) {
                        this.selectedGroup(this.allGroupsGroup);
                    }
                } else {
                    this.refreshGridAndGroup(group.name);
                }
            });
        app.showDialog(deleteGroupVm);
    }

    private updateGroups(receivedGroups: Array<counterGroup>) {
        var deletedGroups = [];

        this.groups().forEach((group: counterGroup) => {
            if (!receivedGroups.first((receivedGroup: counterGroup) => group.name === receivedGroup.name) && group.name !== "All Groups") {
                deletedGroups.push(group);
            }
        });

        this.groups.removeAll(deletedGroups);
        receivedGroups.forEach((receivedGroup: counterGroup) => {
            var foundGroup = this.groups().first((group: counterGroup) => group.name === receivedGroup.name);
            if (!foundGroup) {
                this.groups.push(receivedGroup);
            } else {
                foundGroup.countersCount(receivedGroup.countersCount());
            }
        });

        //if the group is deleted, go to the all groups group
        var currentGroup: counterGroup = this.groups().first(g => g.name === this.selectedGroup().name);
        if (!currentGroup || currentGroup.countersCount() === 0) {
            this.selectedGroup(this.allGroupsGroup);
        }
    }

    private refreshGroupsData() {
        this.groups().forEach((group: counterGroup) => {
            var pagedList = group.getCounters();
            pagedList.invalidateCache();
        });
    }

    private refreshGroups(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var cs = this.activeCounterStorage();

        //TODO: add proper paging support here too
        this.fetchGroups(cs, 0, Number.MAX_VALUE).done(results => {
            this.updateGroups(results);
            this.refreshGroupsData();
            //TODO: add a button to refresh the counters and than use this.refreshCollectionsData();
            deferred.resolve();
        });

        return deferred;
    }

    private reloadCountersData(cs: counterStorage) {
        if (cs.name === this.activeCounterStorage().name) {
            this.refreshGroups().done(() => this.refreshGroupsData());
        }
    }

    selectGroup(group: counterGroup, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            this.selectGroupInternal(group);
        }
    }

    private selectGroupInternal(group: counterGroup) {
        group.activate();
        var countersWithGroupUrl = appUrl.forCounterStorageCounters(group.name, this.activeCounterStorage());
        router.navigate(countersWithGroupUrl, false);
    }

    private getCountersGrid(): virtualTable {
        var gridContents = $(counters.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    private sortGroups() {
        this.groups.sort((c1: counterGroup, c2: counterGroup) => {
            if (c1.isAllGroupsGroup)
                return -1;
            if (c2.isAllGroupsGroup)
                return 1;
            return c1.name.toLowerCase() > c2.name.toLowerCase() ? 1 : -1;
        });
    }

    // Animation callbacks for the groups list
    showGroupElement(element) {
        if (element.nodeType === 1 && counters.isInitialized()) {
            $(element).hide().slideDown(500, () => {
                ko.postbox.publish("SortGroups");
                $(element).highlight();
            });
        }
    }

    hideGroupElement(element) {
        if (element.nodeType === 1) {
            $(element).slideUp(1000, () => { $(element).remove(); });
        }
    }
}

export = counters;
