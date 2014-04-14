import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import searchByQueryCommand = require("commands/filesystem/searchByQueryCommand");
import pagedResultSet = require("common/pagedResultSet");
import pagedList = require("common/pagedList");
import searchSingleInputClause = require("viewmodels/filesystem/searchSingleInputClause");
import searchFileSizeRangeClause = require("viewmodels/filesystem/searchFileSizeRangeClause");
import searchHasMetadataClause = require("viewmodels/filesystem/searchHasMetadataClause");

class search extends viewModelBase {

    private router = router;

    searchUrl = appUrl.forCurrentDatabase().filesystemSearch;
    searchText = ko.observable("");
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();

    static gridSelector = "#filesGrid";

    constructor() {
        super();

        this.searchText.extend({ throttle: 200 }).subscribe(s => this.searchFiles(s));
    }

    canActivate(args: any) {
        return true;
    }

    activate(args) {
        super.activate(args);
        //this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
       
        this.loadFiles(false);
    }

    attached() {


    }

    searchFiles(query: string) {
        var fetcher = (skip: number, take: number) => this.fetchFiles(query, skip, take);
        var list = new pagedList(fetcher);
        this.allFilesPagedItems(list);
    }

    loadFiles(force: boolean) {
        if (!this.allFilesPagedItems() || force) {
            this.allFilesPagedItems(this.createPagedList());
        }

        return this.allFilesPagedItems;
    }

    createPagedList(): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchFiles("", skip, take);
        var list = new pagedList(fetcher);
        return list;
    }

    fetchFiles(query: string, skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new searchByQueryCommand(appUrl.getFilesystem(), query, skip, take, null).execute();
        return task;
    }

    fileNameStartsWith() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Filename starts with: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.searchText("__fileName:" + input + "*"));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    fileNameEndsWith() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Filename ends with: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.searchText("__rfileName:" + String.prototype.reverse(input) + "*"));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    fileSizeBetween() {
        require(["viewmodels/filesystem/searchFileSizeRangeClause"], searchFileSizeRangeClause => {
            var searchFileSizeRangeClauseViewModel: searchFileSizeRangeClause = new searchFileSizeRangeClause();
            searchFileSizeRangeClauseViewModel
                .applyFilterTask
                .done((input: string) => this.searchText(input));
            app.showDialog(searchFileSizeRangeClauseViewModel);
        });
    }

    hasMetadata() {
        require(["viewmodels/filesystem/searchHasMetadataClause"], searchHasMetadataClause => {
            var searchHasMetadataClauseViewModel: searchHasMetadataClause = new searchHasMetadataClause();
            searchHasMetadataClauseViewModel
                .applyFilterTask
                .done((input: string) => this.searchText(input));
            app.showDialog(searchHasMetadataClauseViewModel);
        });
    }
}

export = search;
