import router = require("plugins/router");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import changesContext = require("common/changesContext");
import filesystem = require("models/filesystem/filesystem");
import pagedList = require("common/pagedList");
import getFilesystemFilesCommand = require("commands/filesystem/getFilesCommand");
import getFilesystemRevisionsCommand = require("commands/filesystem/getFilesystemRevisionsCommand");
import createFolderInFilesystem = require("viewmodels/filesystem/files/createFolderInFilesystem");
import treeBindingHandler = require("common/bindingHelpers/treeBindingHandler");
import pagedResultSet = require("common/pagedResultSet");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import file = require("models/filesystem/file");
import folder = require("models/filesystem/folder");
import uploadItem = require("models/filesystem/uploadItem");
import fileUploadBindingHandler = require("common/bindingHelpers/fileUploadBindingHandler");
import uploadQueueHelper = require("common/uploadQueueHelper");
import deleteFilesMatchingQueryConfirm = require("viewmodels/filesystem/deleteFilesMatchingQueryConfirm");
import searchByQueryCommand = require("commands/filesystem/searchByQueryCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import filesystemEditFile = require("viewmodels/filesystem/files/filesystemEditFile");
import fileRenameDialog = require("viewmodels/filesystem/files/fileRenameDialog");
import queryUtil = require("common/queryUtil");

class filesystemFiles extends viewModelBase {

    static revisionsFolderId = "/$$revisions$$";

    appUrls: computedAppUrls;
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();
    selectedFilesText: KnockoutComputed<string>;
    filesCount: KnockoutComputed<number>;

    selectedFolder = ko.observable<string>();
    selectedFolderName: KnockoutComputed<string>;
    addedFolder = ko.observable<folderNodeDto>();
    currentLevelSubdirectories = ko.observableArray<string>();
    uploadFiles = ko.observable<FileList>();
    uploadQueue = ko.observableArray<uploadItem>();
    folderNotificationSubscriptions = {};
    hasFiles: KnockoutComputed<boolean>;
    filesSelection: KnockoutComputed<checkbox>;
    hasAnyFilesSelected: KnockoutComputed<boolean>;
    hasAllFilesSelected: KnockoutComputed<boolean>;
    isAnyFilesAutoSelected = ko.observable<boolean>(false);
    isAllFilesAutoSelected = ko.observable<boolean>(false);
    inRevisionsFolder = ko.observable<boolean>(false);

    anyUploadInProgess: KnockoutComputed<boolean>;
    uploadsStatus: KnockoutComputed<string>;

    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);

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
        treeBindingHandler.includeRevisionsFunc = () => this.activeFilesystem().activeBundles.contains("Versioning");

        this.selectedFilesText = ko.computed(() => {
            if (!!this.selectedFilesIndices()) {
                var documentsText = "file";
                if (this.selectedFilesIndices().length !== 1) {
                    documentsText += "s";
                }
                return documentsText;
            }
            return "";
        });

        this.filesCount = ko.computed(() => {
            if (!!this.allFilesPagedItems()) {
                var p: pagedList = this.allFilesPagedItems();
                return p.totalResultCount();
            }
            return 0;
        });

        this.selectedFolderName = ko.computed(() => {
            if (!this.selectedFolder())
                return "Root Folder";
            var splittedName = this.selectedFolder().split("/");
            return splittedName.last();
        });

        this.hasFiles = ko.computed(() => {
            if (!!this.allFilesPagedItems()) {
                var p: pagedList = this.allFilesPagedItems();
                return p.totalResultCount() > 0;
            }
            return false;
        });

        this.hasAllFilesSelected = ko.computed(() => {
            var filesCount = this.filesCount();
            return filesCount > 0 && filesCount === this.selectedFilesIndices().length;
        });

        this.filesSelection = ko.computed(() => {
            var selected = this.selectedFilesIndices();
            if (this.hasAllFilesSelected()) {
                return checkbox.Checked;
            }
            if (selected.length > 0) {
                return checkbox.SomeChecked;
            }
            return checkbox.UnChecked;
        });

        this.anyUploadInProgess = ko.pureComputed(() => {
            var queue = this.uploadQueue();
            for (var i = 0; i < queue.length; i++) {
                if (queue[i].status() === uploadQueueHelper.uploadingStatus) {
                    return true;
                }
            }
            return false;
        });

        this.uploadsStatus = ko.pureComputed(() => {
            var queue = this.uploadQueue();

            if (queue.length === 0) {
                return 'panel-info';
            }

            var allSuccess = true;

            for (var i = 0; i < queue.length; i++) {
                if (queue[i].status() === uploadQueueHelper.failedStatus) {
                    return 'panel-danger';
                }

                if (queue[i].status() !== uploadQueueHelper.uploadedStatus) {
                    allSuccess = false;
                }
            }

            return allSuccess ? 'panel-success' : 'panel-info';
        });
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("Y1TNKH");

        this.appUrls = appUrl.forCurrentFilesystem();
        this.hasAnyFilesSelected = ko.computed(() => this.selectedFilesIndices().length > 0);

        this.loadFiles();
        this.selectedFolder.subscribe((newValue: string) => this.folderChanged(newValue));

        var storageKeyForFs = uploadQueueHelper.localStorageUploadQueueKey + this.activeFilesystem().name;
        if (window.localStorage.getItem(storageKeyForFs)) {
            this.uploadQueue(
                uploadQueueHelper.parseUploadQueue(
                    window.localStorage.getItem(storageKeyForFs), this.activeFilesystem()));
        }
    }

    attached() {
        super.attached();
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

        this.inRevisionsFolder(newFolder === filesystemFiles.revisionsFolderId);

        // treat notifications events
        if (!newFolder) {
            newFolder = "/";
        }

        if (!this.folderNotificationSubscriptions[newFolder] && changesContext.currentResourceChangesApi() != null) {
            this.folderNotificationSubscriptions[newFolder] = changesContext.currentResourceChangesApi()
                .watchFsFolders(newFolder, (e: fileChangeNotification) => {
                    var callbackFolder = new folder(newFolder);

                    switch (e.Action) {

                    case "Add":
                    {
                        var eventFolder = folder.getFolderFromFilePath(e.File);

                        if (!eventFolder || !treeBindingHandler.isNodeExpanded(filesystemFiles.treeSelector, callbackFolder.path)) {
                            return;
                        }

                        //check if the file is new at the folder level to add it
                        if (callbackFolder.isFileAtFolderLevel(e.File)) {
                            this.loadFiles();
                        } else {
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
                    case "Delete":
                    {
                        var eventFolder = folder.getFolderFromFilePath(e.File);

                        //check if the file is new at the folder level to remove it from the table
                        if (callbackFolder.isFileAtFolderLevel(e.File)) {
                            this.loadFiles();
                        } else {
                            //reload node and its children
                            treeBindingHandler.reloadNode(filesystemFiles.treeSelector, callbackFolder.path);
                        }
                        break;
                    }
                    case "Renaming":
                    {
                        //nothing to do here
                    }
                    case "Renamed":
                    {
                        //reload files to load the new names
                        if (callbackFolder.isFileAtFolderLevel(e.File)) {
                            this.loadFiles();
                        }
                        break;
                    }
                    case "Update":
                    {
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
        if (directory === filesystemFiles.revisionsFolderId) {
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
        var filesGrid = this.getFilesGrid();
        if (!!filesGrid) {
            if (this.hasAnyFilesSelected()) {
                filesGrid.selectNone();
            } else {
                filesGrid.selectSome();
                this.isAnyFilesAutoSelected(this.hasAllFilesSelected() === false);
            }
        }
    }

    selectAll() {
        var filesGrid = this.getFilesGrid();
        if (!!filesGrid && !!this.allFilesPagedItems()) {
            var p: pagedList = this.allFilesPagedItems();
            filesGrid.selectAll(p.totalResultCount());
        }
    }

    selectNone() {
        var filesGrid = this.getFilesGrid();
        if (!!filesGrid) {
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

    refresh() {
        var grid = this.getFilesGrid();
        if (grid) {
            grid.refreshCollectionData();
        }
        this.selectNone();
    }

    deleteSelectedFiles() {
        if (this.hasAllFilesSelected()) {
            this.deleteFolder(false);
        } else {
            var grid = this.getFilesGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    downloadSelectedFiles() {
        var grid = this.getFilesGrid();
        if (grid) {
            var selectedItem = <documentBase>grid.getSelectedItems(1).first();
            var fs = this.activeFilesystem();
            var url = appUrl.forResourceQuery(fs) + "/files/" + encodeURIComponent(selectedItem.getUrl());
            this.downloader.download(fs, url);
        }
    }

    renameSelectedFile() {
        var grid = this.getFilesGrid();
        if (grid) {
            var selectedItem = <file>grid.getSelectedItems(1).first();
            var currentFileName = selectedItem.getId();
            var dialog = new fileRenameDialog(currentFileName, this.activeFilesystem());
            dialog.onExit().done((newName: string) => {
                var currentFilesystemName = this.activeFilesystem().name;
                var recentFilesForCurFilesystem = filesystemEditFile.recentDocumentsInFilesystem().first(x => x.filesystemName === currentFilesystemName);
                if (recentFilesForCurFilesystem) {
                    recentFilesForCurFilesystem.recentFiles.remove(currentFileName);
                }
                selectedItem.setId(newName);
                grid.refreshCollectionData();
            });
            app.showDialog(dialog);
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

    deleteFolder(recursive = true) {
        if (!this.selectedFolder() && recursive) {
            // delete all files from filesystem
            new getFileSystemStatsCommand(this.activeFilesystem())
                .execute()
                .done((fs: filesystemStatisticsDto) => {
                    this.promptDeleteFilesMatchingQuery(fs.FileCount, "");
                });
        } else {
            // Run the query so that we have an idea of what we'll be deleting.
            var query: string;
            if (recursive) {
                query = "__directoryName:" + queryUtil.escapeTerm(this.selectedFolder());
            }else{
                var folder = !this.selectedFolder() ? "/" : this.selectedFolder();
                query = "__directory:" + queryUtil.escapeTerm(folder);
            }

            new searchByQueryCommand(this.activeFilesystem(), query, 0, 1)
                .execute()
                .done((results: pagedResultSet) => {
                if (results.totalResultCount === 0) {
                    app.showMessage("There are no files matching your query.", "Nothing to do");
                } else {
                    this.promptDeleteFilesMatchingQuery(results.totalResultCount, query);
                }
            });
        }
    }

    promptDeleteFilesMatchingQuery(resultCount: number, query: string) {
        var viewModel = new deleteFilesMatchingQueryConfirm(query, resultCount, this.activeFilesystem());
        app.showDialog(viewModel);
        viewModel.deletionTask.done(() => this.loadFiles());
    }
}

export = filesystemFiles;
