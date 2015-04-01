import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import searchByQueryCommand = require("commands/filesystem/searchByQueryCommand");
import pagedResultSet = require("common/pagedResultSet");
import pagedList = require("common/pagedList");
import searchSingleInputClause = require("viewmodels/filesystem/files/searchSingleInputClause");
import searchFileSizeRangeClause = require("viewmodels/filesystem/files/searchFileSizeRangeClause");
import searchHasMetadataClause = require("viewmodels/filesystem/files/searchHasMetadataClause");
import searchLastModifiedBetweenClause = require("viewmodels/filesystem/files/searchLastModifiedBetweenClause");

class search extends viewModelBase {

    private router = router;

    appUrls: computedAppUrls;

    searchUrl = appUrl.forCurrentDatabase().filesystemSearch;
    searchText = ko.observable("");
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();

    static gridSelector = "#filesGrid";

    constructor() {
        super();

        this.searchText.extend({ throttle: 200 }).subscribe(s => this.searchFiles(s));
    }

    activate(args) {
        super.activate(args);

        this.appUrls = appUrl.forCurrentFilesystem();

        this.activeFilesystem.subscribe((fs: filesystem) => {
            this.searchFiles("");
            this.searchText("");
        });
       
        this.loadFiles();
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
        this.allFilesPagedItems(this.createPagedList(query));
    }

    loadFiles() {
        this.allFilesPagedItems(this.createPagedList(""));
    }

    createPagedList(query: string): pagedList {
        var fetcher = (skip: number, take: number) => this.fetchFiles(query, skip, take);
        return new pagedList(fetcher);
    }

    fetchFiles(query: string, skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new searchByQueryCommand(appUrl.getFileSystem(), query, skip, take, null).execute();
        return task;
    }

    fileNameStartsWith() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Filename starts with: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput("__fileName:" + this.escapeQueryString(input) + "*"));
            app.showDialog(searchSingleInputClauseViewModel);
        });
    }

    fileNameEndsWith() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Filename ends with: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput("__rfileName:" + String.prototype.reverse(this.escapeQueryString(input)) + "*"));
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
                .done((input: string) => this.addToSearchInput(this.escapeQueryString(input)));
            app.showDialog(searchHasMetadataClauseViewModel);
        });
    }

    inFolder() {
        require(["viewmodels/filesystem/searchSingleInputClause"], searchSingleInputClause => {
            var searchSingleInputClauseViewModel: searchSingleInputClause = new searchSingleInputClause("Folder path: ");
            searchSingleInputClauseViewModel
                .applyFilterTask
                .done((input: string) => this.addToSearchInput("__directoryName:/" + this.escapeQueryString(input)));
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

    private escapeQueryString(query: string) : string {
        return query.replace(/([ /\-\_\.])/g, '\\$1');
    }
}

export = search;
