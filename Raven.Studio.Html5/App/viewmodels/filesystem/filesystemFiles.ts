import router = require("plugins/router");
import appUrl = require("common/appUrl");
import app = require("durandal/app");
import filesystem = require("models/filesystem/filesystem");
import pagedList = require("common/pagedList");
import getFilesystemFilesCommand = require("commands/filesystem/getFilesCommand");
import createFolderInFilesystem = require("viewmodels/filesystem/createFolderInFilesystem");
import treeBindingHandler = require("common/treeBindingHandler");
import pagedResultSet = require("common/pagedResultSet");
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import file = require("models/filesystem/file");
import uploadItem = require("models/uploadItem");
import fileUploadBindingHandler = require("common/fileUploadBindingHandler");
import uploadQueueHelper = require("common/uploadQueueHelper");

class filesystemFiles extends viewModelBase {

   
    fileName = ko.observable<file>();
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();
    isSelectAll = ko.observable(false);
    hasAnyFileSelected: KnockoutComputed<boolean>;
    selectedFolder = ko.observable<string>();
    addedFolder = ko.observable<folderNodeDto>();
    currentLevelSubdirectories = ko.observableArray<string>();
    uploadFiles = ko.observable<FileList>();
    uploadQueue = ko.observableArray<uploadItem>();
    private activeFilesystemSubscription: any;


    static treeSelector = "#filesTree";
    static gridSelector = "#filesGrid";
    static uploadQueuePanelToggleSelector = "#uploadQueuePanelToggle"
    static uploadQueueSelector = "#uploadQueue";
    static uploadQueuePanelCollapsedSelector = "#uploadQueuePanelCollapsed";

    constructor() {
        super();

        this.uploadQueue.subscribe(x => uploadQueueHelper.updateLocalStorage(x, this.activeFilesystem()));
        fileUploadBindingHandler.install();
        treeBindingHandler.install();
    }

    canActivate(args: any) {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
        this.hasAnyFileSelected = ko.computed(() => this.selectedFilesIndices().length > 0);

        this.loadFiles(false);
        this.selectedFolder.subscribe((newValue: string) => this.loadFiles(true));

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

        this.activeFilesystemSubscription.dispose();
    }

    loadFiles(force: boolean) {
        this.allFilesPagedItems(this.createPagedList(this.selectedFolder()));

        return this.allFilesPagedItems;
    }

    createPagedList(directory): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchFiles(directory, skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    fetchFiles(directory: string, skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getFilesystemFilesCommand(appUrl.getFilesystem(), directory, skip, take).execute();

        return task;
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            var filesystemFilesUrl = appUrl.forFilesystemFiles(fs);
            this.navigate(filesystemFilesUrl);

            this.loadFiles(true);
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
            filesGrid.selectAll();
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
                addClass: "temp-folder"
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
    }

    downloadSelectedFiles() {
        var grid = this.getFilesGrid();
        if (grid) {
            var selectedItem = <documentBase>grid.getSelectedItems(1).first();
            var url = appUrl.forResourceQuery(this.activeFilesystem()) + "/files/" + selectedItem.getId();
            window.location.assign(url);
        }
    }

    modelPolling() {
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
        uploadQueueHelper.updateQueueStatus(x.id(), "Uploaded", this.uploadQueue());
        var lastSlash = x.fileName().lastIndexOf("/");
        if (lastSlash > 0) {
            var directory = x.fileName().substring(0, lastSlash);
            treeBindingHandler.updateNodeHierarchyStyle(filesystemFiles.treeSelector, directory, "");
        }

        this.loadFiles(true);
    }

    uploadFailed(x: uploadItem) {
        ko.postbox.publish("UploadFileStatusChanged", x);
        uploadQueueHelper.updateQueueStatus(x.id(), "Failed", this.uploadQueue());
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
