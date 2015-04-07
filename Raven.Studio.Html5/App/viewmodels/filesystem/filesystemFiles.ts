import router = require("plugins/router");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import shell = require("viewmodels/shell");

import filesystem = require("models/filesystem/filesystem");
import pagedList = require("common/pagedList");
import getFilesystemFilesCommand = require("commands/filesystem/getFilesCommand");
import getFilesystemRevisionsCommand = require('commands/filesystem/getFilesystemRevisionsCommand');
import createFolderInFilesystem = require("viewmodels/filesystem/createFolderInFilesystem");
import treeBindingHandler = require("common/treeBindingHandler");
import pagedResultSet = require("common/pagedResultSet");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import file = require("models/filesystem/file");
import folder = require("models/filesystem/folder");
import uploadItem = require("models/uploadItem");
import fileUploadBindingHandler = require("common/fileUploadBindingHandler");
import uploadQueueHelper = require("common/uploadQueueHelper");

class filesystemFiles extends viewModelBase {

    static revisionsFolderId = "/$$revisions$$";

    appUrls: computedAppUrls;
    fileName = ko.observable<file>();
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();
    selectedFilesText: KnockoutComputed<string>;
    hasFiles: KnockoutComputed<boolean>;
    isSelectAll = ko.observable(false);
    hasAnyFileSelected: KnockoutComputed<boolean>;
    selectedFolder = ko.observable<string>();
    addedFolder = ko.observable<folderNodeDto>();
    currentLevelSubdirectories = ko.observableArray<string>();
    uploadFiles = ko.observable<FileList>();
    uploadQueue = ko.observableArray<uploadItem>();
    folderNotificationSubscriptions = {};
    hasAnyFilesSelected: KnockoutComputed<boolean>;
    hasAllFilesSelected: KnockoutComputed<boolean>;

    inRevisionsFolder = ko.observable<boolean>(false);

    private activeFilesystemSubscription: any;

    static treeSelector = "#filesTree";
    static gridSelector = "#filesGrid";
    static uploadQueuePanelToggleSelector = "#uploadQueuePanelToggle";
    static uploadQueueSelector = "#uploadQueue";
    static uploadQueuePanelCollapsedSelector = "#uploadQueuePanelCollapsed";

    constructor() {
        super();

        this.uploadQueue.subscribe(x => this.newUpload(x));
        fileUploadBindingHandler.install();
        treeBindingHandler.install();
        treeBindingHandler.includeRevisionsFunc = () => this.activeFilesystem().activeBundles.contains('Versioning');

        this.selectedFilesText = ko.computed(() => {
            if (!!this.selectedFilesIndices()) {
                var documentsText = "file";
                if (this.selectedFilesIndices().length != 1) {
                    documentsText += "s";
                }
                return documentsText;
            }
            return "";
        });

        this.hasFiles = ko.computed(() => {
            if (!!this.allFilesPagedItems()) {
                var p: pagedList = this.allFilesPagedItems();
                return p.totalResultCount() > 0;
            }
            return false;
        });
    }

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();
        this.hasAnyFileSelected = ko.computed(() => this.selectedFilesIndices().length > 0);

        this.loadFiles();
        this.selectedFolder.subscribe((newValue: string) => this.folderChanged(newValue));

        var storageKeyForFs = uploadQueueHelper.localStorageUploadQueueKey + this.activeFilesystem().name;
        if (window.localStorage.getItem(storageKeyForFs)) {
            this.uploadQueue(
                uploadQueueHelper.parseUploadQueue(
                    window.localStorage.getItem(storageKeyForFs), this.activeFilesystem()));
        }
    }

    attached(view, parent) {
        this.collapseUploadQueuePanel();
    }

    deactivate() {
        super.deactivate();
    }

    loadFiles() {
        this.allFilesPagedItems(this.createPagedList(this.selectedFolder()));
    }

    folderChanged(newFolder: string) {
        this.loadFiles();

        this.inRevisionsFolder(newFolder == filesystemFiles.revisionsFolderId);

        // treat notifications events
        if (!newFolder) {
            newFolder = "/";
        }

        if (!this.folderNotificationSubscriptions[newFolder]) {
            this.folderNotificationSubscriptions[newFolder] = shell.currentResourceChangesApi()
                .watchFsFolders(newFolder, (e: fileChangeNotification) => {
                    var callbackFolder = new folder(newFolder);
                    if (!callbackFolder)
                        return;
                    switch (e.Action) {

                        case "Add": {
                            var eventFolder = folder.getFolderFromFilePath(e.File);

                            if (!eventFolder || !treeBindingHandler.isNodeExpanded(filesystemFiles.treeSelector, callbackFolder.path)) {
                                return;
                            }

                            //check if the file is new at the folder level to add it
                            if (callbackFolder.isFileAtFolderLevel(e.File)) {
                                this.loadFiles();
                            }
                            else {
                                //check if a new folder at this level was added so we add it to the tree
                                var subPaths = eventFolder.getSubpathsFrom(callbackFolder.path);
                                if (subPaths.length > 1 && !treeBindingHandler.nodeExists(filesystemFiles.treeSelector, subPaths[1].path)) {
                                    var newNode = {
                                        key: subPaths[1].path,
                                        title: subPaths[1].name,
                                        isLazy: true,
                                        isFolder: true,
                                    };
                                    this.addedFolder(newNode);
                                }
                            }

                            break;
                        }
                        case "Delete": {
                            var eventFolder = folder.getFolderFromFilePath(e.File);

                            //check if the file is new at the folder level to remove it from the table
                            if (callbackFolder.isFileAtFolderLevel(e.File)) {
                                this.loadFiles();
                            }
                            else {
                                //reload node and its children
                                treeBindingHandler.reloadNode(filesystemFiles.treeSelector, callbackFolder.path);
                            }
                            break;
                        }
                        case "Renaming": {
                            //nothing to do here
                        }
                        case "Renamed": {
                            //reload files to load the new names
                            if (callbackFolder.isFileAtFolderLevel(e.File)) {
                                this.loadFiles();
                            }
                            break;
                        }
                        case "Update": {
                            //check if the file is new at the folder level to add it
                            if (callbackFolder.isFileAtFolderLevel(e.File)) {
                                this.loadFiles();
                            }
                            break;
                        }
                        default:
                            console.error("unknown notification action");
                    }
                });
        }
    }

    createPagedList(directory): pagedList {
        var fetcher;
        if (directory == filesystemFiles.revisionsFolderId) { 
            fetcher = (skip: number, take: number) => this.fetchRevisions(skip, take);
        } else {
            fetcher = (skip: number, take: number) => this.fetchFiles(directory, skip, take);
        }

        var list = new pagedList(fetcher);
        return list;
    }

    fetchFiles(directory: string, skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getFilesystemFilesCommand(appUrl.getFileSystem(), directory, skip, take).execute();
        return task;
    }

    fetchRevisions(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getFilesystemRevisionsCommand(appUrl.getFileSystem(), skip, take).execute();
        return task;
    }

    fileSystemChanged(fs: filesystem) {
        if (!!fs) {
            this.loadFiles();
        }
    }
    editSelectedFile() {
        var grid = this.getFilesGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    toggleSelectAll() {
        this.isSelectAll.toggle();

        var filesGrid = this.getFilesGrid();
        if (filesGrid && this.isSelectAll()) {
            filesGrid.selectSome();
        } else if (filesGrid && !this.isSelectAll()) {
            filesGrid.selectNone();
        }
    }

    getFilesGrid(): virtualTable {
        var gridContents = $(filesystemFiles.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    createFolder() {
        var createFolderVm = new createFolderInFilesystem(this.currentLevelSubdirectories());
        createFolderVm.creationTask.done((folderName: string) => {
            var parentDirectory = this.selectedFolder() ? this.selectedFolder() : "";
            var newNode = {
                key: parentDirectory + "/" + folderName,
                title: folderName,
                isLazy: true,
                isFolder: true,
                addClass: treeBindingHandler.transientNodeStyle
            };
            this.addedFolder(newNode);
        });

        app.showDialog(createFolderVm);
    }

    deleteSelectedFiles() {
        var grid = this.getFilesGrid();
        if (grid) {
            grid.deleteSelectedItems();
        }

        this.isSelectAll(false);
    }

    downloadSelectedFiles() {
        var grid = this.getFilesGrid();
        if (grid) {
            var selectedItem = <documentBase>grid.getSelectedItems(1).first();
            var selectedFolder = this.selectedFolder();

            if (selectedFolder == null)
                selectedFolder = "";

            var url = appUrl.forResourceQuery(this.activeFilesystem()) + "/files" + selectedFolder + "/" + selectedItem.getId();
            window.location.assign(url);
        }
    }

    clearUploadQueue() {
        window.localStorage.removeItem(uploadQueueHelper.localStorageUploadQueueKey + this.activeFilesystem().name);
        this.uploadQueue.removeAll();
    }

    navigateToFiles() {
        router.navigate(appUrl.forFilesystemFiles(this.activeFilesystem()));
    }

    uploadSuccess(x: uploadItem) {
        ko.postbox.publish("UploadFileStatusChanged", x);
        uploadQueueHelper.updateQueueStatus(x.id(), uploadQueueHelper.uploadedStatus, this.uploadQueue());
        this.uploadQueue(uploadQueueHelper.sortUploadQueue(this.uploadQueue()));
        var persistedFolder = folder.getFolderFromFilePath(x.fileName());
        if (persistedFolder) {
            treeBindingHandler.updateNodeHierarchyStyle(filesystemFiles.treeSelector, persistedFolder.path, "");
            treeBindingHandler.setNodeLoadStatus(filesystemFiles.treeSelector, persistedFolder.path, 0);
        }

        this.loadFiles();
    }

    uploadFailed(x: uploadItem) {
        ko.postbox.publish("UploadFileStatusChanged", x);
        uploadQueueHelper.updateQueueStatus(x.id(), uploadQueueHelper.failedStatus, this.uploadQueue());
        this.uploadQueue(uploadQueueHelper.sortUploadQueue(this.uploadQueue()));
    }

    newUpload(x: uploadItem[]) {
        if (x && x.length > 0) {
            uploadQueueHelper.updateLocalStorage(x, this.activeFilesystem())
            var waitingItems = x.filter(x => x.status() === uploadQueueHelper.queuedStatus || x.status() === uploadQueueHelper.uploadingStatus);
            for (var i = 0; i < waitingItems.length; i++) {
                var persistedFolder = folder.getFolderFromFilePath(waitingItems[i].fileName());
                treeBindingHandler.setNodeLoadStatus(filesystemFiles.treeSelector, persistedFolder.path, 1);
            }
        }
    }

    toggleCollapseUploadQueue() {
        if ($(filesystemFiles.uploadQueuePanelToggleSelector).hasClass('opened')) {
            this.collapseUploadQueuePanel();
        } else {
            this.expandUploadQueuePanel();
        }
    }

    expandUploadQueuePanel() {
        $(filesystemFiles.uploadQueuePanelToggleSelector).addClass("opened").find('i').removeClass('fa-angle-double-up').addClass('fa-angle-double-down');
        $(filesystemFiles.uploadQueueSelector).removeClass("hidden");
        $(filesystemFiles.uploadQueuePanelCollapsedSelector).addClass("hidden");
        $(".upload-queue").removeClass("upload-queue-min");
    }

    collapseUploadQueuePanel() {
        $(filesystemFiles.uploadQueuePanelToggleSelector).removeClass('opened').find('i').removeClass('fa-angle-double-down').addClass('fa-angle-double-up');
        $(filesystemFiles.uploadQueueSelector).addClass("hidden");
        $(filesystemFiles.uploadQueuePanelCollapsedSelector).removeClass("hidden");
        $(".upload-queue").addClass("upload-queue-min");
    }
}

export = filesystemFiles;
