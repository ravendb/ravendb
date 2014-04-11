import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import pagedList = require("common/pagedList");
import getFilesystemFilesCommand = require("commands/filesystem/getFilesCommand");
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

    static gridSelector = "#filesGrid";

    canActivate(args: any) {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
        this.hasAnyFileSelected = ko.computed(() => this.selectedFilesIndices().length > 0);

        this.loadFiles(false);
    }

    loadFiles(force: boolean) {
        if (!this.allFilesPagedItems() || force ) {
            this.allFilesPagedItems(this.createPagedList());
        }

        return this.allFilesPagedItems;
    }

    createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchFiles(skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    fetchFiles(skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new getFilesystemFilesCommand(appUrl.getFilesystem(), skip, take).execute();
        //task.done((results: pagedResultSet) => this.documentCount(results.totalResultCount));

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

    deleteSelectedFiles() {
        var grid = this.getFilesGrid();
        if (grid) {
            grid.deleteSelectedItems();
        }
    }

    uploadFile() {
        router.navigate(appUrl.forFilesystemUploadFile(this.activeFilesystem()));
    }
}

export = filesystemFiles;
