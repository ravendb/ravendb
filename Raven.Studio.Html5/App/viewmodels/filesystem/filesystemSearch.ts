import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import searchByQueryCommand = require("commands/filesystem/searchByQueryCommand");
import pagedResultSet = require("common/pagedResultSet");
import pagedList = require("common/pagedList");

class filesystemSearch extends viewModelBase {

    private router = router;

    searchUrl = appUrl.forCurrentDatabase().filesystemSearch;
    searchText = ko.observable("");
    allFilesPagedItems = ko.observable<pagedList>();
    selectedFilesIndices = ko.observableArray<number>();
    //startsWithText = ko.observable("");

    static gridSelector = "#filesGrid";

    constructor() {
        super();

        this.searchText.extend({ throttle: 200 }).subscribe(s => this.searchFiles(s));
        //this.startsWithText.extend({ throttle: 200 }).subscribe(s => this.fileNameStartsWithClauseBuilder(s));
    }

    canActivate(args: any) {
        return true;
    }

    attached() {

        (<any>$("body")).click((event) => {
            $('.popover-html').each((index, value) => {
                var elem = (<any>$(value));
                if (!elem.is(event.target) && elem.has(event.target).length === 0 && $('.popover').has(event.target).length === 0) {
                    elem.popover('hide');
                }
            });
        });

        var popoverTemplate = '<div class="popover popover-lg"><div class="arrow"></div><div class="popover-inner"><h3 class="popover-title"></h3><div class="popover-content"><p></p></div></div></div>';

        ($('#fileNameEndsWith')).popover({
            html: true,
            content: () => $('#fileNameEndsWithContextMenu').html(),
            placement: 'bottom'
        });

        ($('#fileNameStartsWith')).popover({
            html: true,
            content: () => $('#fileNameStartsWithContextMenu').html(),
            placement: 'bottom'
        });

        ($('#fileSizeBetween')).popover({
            html: true,
            content: () => $('#fileSizeBetweenContextMenu').html(),
            placement: 'bottom',
            template: popoverTemplate
        });

        ($('#hasMetadata')).popover({
            html: true,
            content: () => $('#hasMetadataContextMenu').html(),
            placement: 'bottom',
            template: popoverTemplate
        });

        ($('#inFolder')).popover({
            html: true,
            content: () => $('#inFolderContextMenu').html(),
            placement: 'bottom'
        });

        ($('#lastModifiedBetween')).popover({
            html: true,
            content: () => $('#lastModifiedBetweenContextMenu').html(),
            placement: 'bottom',
            template: popoverTemplate
        });
    }

    searchFiles(query: string) {
        var fetcher = (skip: number, take: number) => this.fetchFiles(query.toLowerCase(), skip, take);
        var list = new pagedList(fetcher);
        this.allFilesPagedItems(list);
    }

    fetchFiles(query: string, skip: number, take: number): JQueryPromise<pagedResultSet> {
        var task = new searchByQueryCommand(appUrl.getFilesystem(), query, skip, take, null).execute();
        return task;
    }

    fileNameStartsWithClauseBuilder(input: string) {
        this.searchText("__fileName:" + input + "*");
    }
}

export = filesystemSearch;
