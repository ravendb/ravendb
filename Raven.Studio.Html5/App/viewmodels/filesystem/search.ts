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
import searchLastModifiedBetweenClause = require("viewmodels/filesystem/searchLastModifiedBetweenClause");

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
        this.activeFilesystem.subscribe((fs: filesystem) => {
            this.searchFiles("");
            this.searchText("");
        });
       
        this.loadFiles(false);
    }

    attached() {
    }

    clear() {
        this.searchText("");
    }

    search() {
        this.searchFiles(this.searchText());
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
                .done((input: string) => this.addToSearchInput("__fileName:" + input + "*"));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    fileNameEndsWith() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Filename ends with: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput("__rfileName:" + String.prototype.reverse(input) + "*"));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    fileSizeBetween() {
        require(["viewmodels/filesystem/searchFileSizeRangeClause"], searchFileSizeRangeClause => {
            var searchFileSizeRangeClauseViewModel: searchFileSizeRangeClause = new searchFileSizeRangeClause();
            searchFileSizeRangeClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput(input));
            app.showDialog(searchFileSizeRangeClauseViewModel);
        });
    }

    hasMetadata() {
        require(["viewmodels/filesystem/searchHasMetadataClause"], searchHasMetadataClause => {
            var searchHasMetadataClauseViewModel: searchHasMetadataClause = new searchHasMetadataClause(this.activeFilesystem());
            searchHasMetadataClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput(input));
            app.showDialog(searchHasMetadataClauseViewModel);
        });
    }

    inFolder() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Folder path: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput("__directory:" + input));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    lastModifiedBetween() {
        require(["viewmodels/filesystem/searchLastModifiedBetweenClause"], searchLastModifiedBetweenClause => {
            var searchLastModifiedBetweenClauseViewModel: searchLastModifiedBetweenClause = new searchLastModifiedBetweenClause();
            searchLastModifiedBetweenClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput(input));
            app.showDialog(searchLastModifiedBetweenClauseViewModel);
        });
    }

    addToSearchInput(input: string) {
        var currentSearchText = this.searchText();
        if (currentSearchText != null && currentSearchText.trim().length > 0)
            currentSearchText += " AND ";
        this.searchText(currentSearchText + input);
    }
}

export = search;
