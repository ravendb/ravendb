import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");


import viewModelBase = require("viewmodels/viewModelBase");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");

import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");
import changeSubscription = require('common/changeSubscription');
import customFunctions = require("models/database/documents/customFunctions");
import customColumns = require('models/database/documents/customColumns');
import customColumnParams = require('models/database/documents/customColumnParams');

import getCollectionsCommand = require("commands/database/documents/getCollectionsCommand");
import getCustomColumnsCommand = require('commands/database/documents/getCustomColumnsCommand');
import getOperationStatusCommand = require('commands/operations/getOperationStatusCommand');

import selectColumns = require("viewmodels/common/selectColumns");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");






import shell = require("viewmodels/shell");
import counterStorage = require("models/counter/counterStorage");
import getCounterGroupsCommand = require("commands/counter/getCounterGroupsCommand");
import counterGroup = require("models/counter/counterGroup");
import editCounterDialog = require("viewmodels/counter/editCounterDialog");
import counterChange = require("models/counter/counterChange");
import updateCounterCommand = require("commands/counter/updateCounterCommand");

class counters extends viewModelBase {

    groups = ko.observableArray<counterGroup>();
    allGroupsGroup: counterGroup;
    selectedGroup = ko.observable<counterGroup>().subscribeTo("ActivateGroup").distinctUntilChanged();
    groupToSelectName: string;
    selectedCounterIndices = ko.observableArray<number>();
    hasCounters: KnockoutComputed<boolean>;
    hasAnyCountersSelected: KnockoutComputed<boolean>;
    hasAllCountersSelected: KnockoutComputed<boolean>;

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    static gridSelector = "#countersGrid";





    displayName = "documents";

    currentCollectionPagedItems = ko.observable<pagedList>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentCustomFunctions = ko.observable<customFunctions>(customFunctions.empty());
   
    selectedCountersText: KnockoutComputed<string>;
    
    contextName = ko.observable<string>('');
    currentCollection = ko.observable<collection>();
    isRegularCollection: KnockoutComputed<boolean>;


    isAnyDocumentsAutoSelected = ko.observable<boolean>(false);
    isAllDocumentsAutoSelected = ko.observable<boolean>(false);

    lastCollectionCountUpdate = ko.observable<string>();
    alerts = ko.observable<alert[]>([]);
    token = ko.observable<singleAuthToken>();


    constructor() {
        super();

        this.selectedGroup.subscribe(c => this.selectedGroupChanged(c));

        this.hasCounters = ko.computed(() => {
            var selectedGroup: counterGroup = this.selectedGroup();
            if (!!selectedGroup) {
                if (selectedGroup.name === collection.allDocsCollectionName) {
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

        this.isRegularCollection = ko.computed(() => {
            var group: counterGroup = this.selectedGroup();
            return !!group && !group.isAllGroupsGroup;
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
    }

    activate(args) {
        super.activate(args);

        //TODO: update this in documentation
        //this.updateHelpLink('G8CDCP');

        // We can optionally pass in a group name to view's URL, e.g. #counterstorages/counters?group=Foo&counterstorage=test
        this.groupToSelectName = args ? args.group : null;

        var cs = this.activeCounterStorage();
        this.fetchGroups().done(results => this.collectionsLoaded(results, cs));
    }


    attached() {
        /*super.createKeyboardShortcut("F2", () => this.editSelectedCounter(), counters.gridSelector);*/

        // Q. Why do we have to setup the grid shortcuts here, when the grid already catches these shortcuts?
        // A. Because if the focus isn't on the grid, but on the docs page itself, we still need to catch the shortcuts.
        /*var docsPageSelector = ".documents-page";
        this.createKeyboardShortcut("DELETE", () => this.getDocumentsGrid().deleteSelectedItems(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, D", () => this.copySelectedDocs(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, I", () => this.copySelectedDocIds(), docsPageSelector);*/
    }

    /*private fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db, this.collections(), this.lastCollectionCountUpdate).execute();
    }*/

    createNotifications(): Array<changeSubscription> {
        return [
            //TODO: create subscription to all coutners
            //shell.currentResourceChangesApi().watchAllCounters(() => this.fetchGroups())
        ];
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            //ko.postbox.subscribe("EditItem", () => this.editSelectedDoc()),
            //ko.postbox.subscribe("ChangesApiReconnected", (cs: database) => this.reloadCountersData(cs))
        ];
    }

    private fetchGroups(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var cs = this.activeCounterStorage();

        var getGroupsCommand = new getCounterGroupsCommand(cs);
        getGroupsCommand.execute().done((results: counterGroup[]) => {
            deferred.resolve(results);
        });

        return deferred;
    }

    collectionsLoaded(groups: Array<counterGroup>, cs: counterStorage) {
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
        var counterChangeVm = new editCounterDialog();
        counterChangeVm.updateTask.done((change: counterChange, isNew: boolean) => {
            var counterCommand = new updateCounterCommand(this.activeCounterStorage(), change.group(), change.counterName(), change.delta(), isNew);
            counterCommand.execute();
        });
        app.showDialog(counterChangeVm);
    }

    editSelectedCounter() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    change() {
        
    }

    reset() {
        
    }

    //TODO: this binding has notification leak!
    private selectedGroupChanged(selected: counterGroup) {
        /*if (selected) {
            var customColumnsCommand = selected.isAllDocuments ?
                getCustomColumnsCommand.forAllDocuments(this.activeDatabase()) : getCustomColumnsCommand.forCollection(selected.name, this.activeDatabase());

            this.contextName(customColumnsCommand.docName);

            customColumnsCommand.execute().done((dto: customColumnsDto) => {
                if (dto) {
                    this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                    this.currentColumnsParams().customMode(true);
                } else {
                    // use default values!
                    this.currentColumnsParams().columns.removeAll();
                    this.currentColumnsParams().customMode(false);
                }

                var pagedList = selected.getDocuments();
                this.currentCollectionPagedItems(pagedList);
                this.currentCollection(selected);
            });
        }*/
    }

    deleteGroup(collection: collection) {
        /*if (collection) {
            var viewModel = new deleteCollection(collection);
            viewModel.deletionTask.done((result: operationIdDto) => {
                if (!collection.isAllDocuments) {
                    this.collections.remove(collection);

                    var selectedCollection: collection = this.selectedCollection();
                    if (collection.name == selectedCollection.name) {
                        this.selectCollection(this.allDocumentsCollection);
                    }
                } else {
                    this.selectNone();
                }

                this.updateGridAfterOperationComplete(collection, result.OperationId);
            });
            app.showDialog(viewModel);
        }*/
    }

    private updateGridAfterOperationComplete(collection: collection, operationId: number) {
        /*var getOperationStatusTask = new getOperationStatusCommand(collection.ownerDatabase, operationId);
        getOperationStatusTask.execute()
            .done((result: bulkOperationStatusDto) => {
                if (result.Completed) {
                    var selectedCollection: collection = this.selectedCollection();

                    if (selectedCollection.isAllDocuments) {
                        var docsGrid = this.getDocumentsGrid();
                        docsGrid.refreshCollectionData();
                    } else {
                        var allDocumentsPagedList = this.allDocumentsCollection.getDocuments();
                        allDocumentsPagedList.invalidateCache();
                    }
                } else {
                    setTimeout(() => this.updateGridAfterOperationComplete(collection, operationId), 500);
                }
            });*/
    }

    private updateGroups(receivedGroups: Array<collection>) {
        /*var deletedCollections = [];

        this.collections().forEach((col: collection) => {
            if (!receivedGroups.first((receivedCol: collection) => col.name == receivedCol.name) && col.name != 'System Documents' && col.name != 'All Documents') {
                deletedCollections.push(col);
            }
        });

        this.collections.removeAll(deletedCollections);

        receivedGroups.forEach((receivedCol: collection) => {
            var foundCollection = this.collections().first((col: collection) => col.name == receivedCol.name);
            if (!foundCollection) {
                this.collections.push(receivedCol);
            } else {
                foundCollection.documentCount(receivedCol.documentCount());
            }
        });

        //if the collection is deleted, go to the all documents collection
        var currentCollection: collection = this.collections().first(c => c.name === this.selectedCollection().name);
        if (!currentCollection || currentCollection.documentCount() == 0) {
            this.selectCollection(this.allDocumentsCollection);
        }*/
    }

    private refreshCollectionsData() {
        /*var selectedCollection: collection = this.selectedCollection();

        this.collections().forEach((collection: collection) => {
            if (collection.name == selectedCollection.name) {
                var docsGrid = this.getDocumentsGrid();
                if (!!docsGrid) {
                    docsGrid.refreshCollectionData();
                }
            } else {
                var pagedList = collection.getDocuments();
                pagedList.invalidateCache();
            }
        });*/
    }

    private reloadCountersData(cs: counterStorage) {
        /*if (cs.name === this.activeCounterStorage().name) {
            this.refreshCollections().done(() => {
                this.refreshCollectionsData();
            });
        }*/
    }

    selectGroup(group: counterGroup, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            group.activate();
            var documentsWithCollectionUrl = appUrl.forDocuments(group.name, this.activeDatabase());
            router.navigate(documentsWithCollectionUrl, false);
        }
    }

    /*selectColumns() {
        // Fetch column widths from virtual table
        var virtualTable = this.getDocumentsGrid();
        var vtColumns = virtualTable.columns();
        this.currentColumnsParams().columns().forEach((column: customColumnParams) => {
            for (var i = 0; i < vtColumns.length; i++) {
                if (column.binding() === vtColumns[i].binding) {
                    column.width(vtColumns[i].width() | 0);
                    break;
                }
            }
        });

        var selectColumnsViewModel = new selectColumns(this.currentColumnsParams().clone(), this.currentCustomFunctions(), this.contextName(), this.activeDatabase());
        app.showDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols) => {
            this.currentColumnsParams(cols);

            var pagedList = this.currentCollection().getDocuments();
            this.currentCollectionPagedItems(pagedList);
        });
    }*/

    toggleSelectAll() {
        var docsGrid = this.getDocumentsGrid();

        if (!!docsGrid) {
            if (this.hasAnyCountersSelected()) {
                docsGrid.selectNone();
            } else {
                docsGrid.selectSome();

                this.isAnyDocumentsAutoSelected(this.hasAllCountersSelected() == false);
            }
        }
    }

    selectAll() {
        /*var docsGrid = this.getDocumentsGrid();
        var c: collection = this.selectedCollection();

        if (!!docsGrid && !!c) {
            docsGrid.selectAll(c.documentCount());
        }*/
    }

    selectNone() {
        var docsGrid = this.getDocumentsGrid();

        if (!!docsGrid) {
            docsGrid.selectNone();
        }
    }

    deleteSelectedDocs() {
        /*if (!this.selectedCollection().isSystemDocuments && this.hasAllCountersSelected()) {
            this.deleteGroup(this.selectedCollection());
        }
        else {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }*/
    }

    copySelectedDocs() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocs();
        }
    }

    copySelectedDocIds() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocIds();
        }
    }

    private getDocumentsGrid(): virtualTable {
        var gridContents = $(counters.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    urlForAlert(alert: alert) {
        var index = this.alerts().indexOf(alert);
        return appUrl.forAlerts(this.activeDatabase()) + "&item=" + index;
    }
}

export = counters;