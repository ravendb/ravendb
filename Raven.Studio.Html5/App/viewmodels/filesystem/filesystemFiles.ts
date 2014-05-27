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

class filesystemFiles extends viewModelBase {

   
    fileName = ko.observable<file>();
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();
    isSelectAll = ko.observable(false);
    hasAnyFileSelected: KnockoutComputed<boolean>;
    selectedFolder = ko.observable<string>();
    addedFolder = ko.observable<string>();
    currentLevelSubdirectories = ko.observableArray<string>();
    private activeFilesystemSubscription: any;

    static gridSelector = "#filesGrid";

    canActivate(args: any) {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
        this.hasAnyFileSelected = ko.computed(() => this.selectedFilesIndices().length > 0);

        this.loadFiles(false);
        this.selectedFolder.subscribe((newValue: string) => this.loadFiles(true));
        treeBindingHandler.install();
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
        createFolderVm.creationTask.done((folderName : string) => {
            this.addedFolder(folderName);
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

    uploadFile() {
        router.navigate(appUrl.forFilesystemUploadFile(this.activeFilesystem(), this.selectedFolder()));
    }

    modelPolling() {
    }
}

export = filesystemFiles;
