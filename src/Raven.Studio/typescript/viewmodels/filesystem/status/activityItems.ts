import synchronizationDetail = require("models/filesystem/synchronizationDetail");
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import filesystem = require("models/filesystem/filesystem");

class activityItems {
    
    private pageSize = 50;

    totalCount = ko.observable<number>();
    pageIndex = ko.observable(0);

    items = ko.observableArray<synchronizationDetail>();

    constructor(private fs: filesystem, private activity: synchronizationActivity, private direction: synchronizationDirection) {
        this.retrieveItems();
    }

    maxPageIndex = ko.computed(() => {
        return Math.ceil(this.totalCount() / this.pageSize) - 1;
    });

    allPages = ko.computed(() =>{
        var pages: { pageNumber: number}[] = [];

        for (var i = 0; i <= this.maxPageIndex(); i++) {
            pages.push({ pageNumber: (i + 1) });
        }

        return pages;
    });

    moveToPage(index: number) {
        this.pageIndex(index);
        this.retrieveItems();
    };

    refresh() {
        this.retrieveItems();
    }

    private retrieveItems() {
        var start = this.pageIndex() * this.pageSize;

        var command: typeof getSyncOutgoingActivitiesCommand | typeof getSyncIncomingActivitiesCommand;

        switch (this.direction) {
            case synchronizationDirection.Outgoing:
                command = getSyncOutgoingActivitiesCommand;
                break;
            case synchronizationDirection.Incoming:
                command = getSyncIncomingActivitiesCommand;
                break;
            default:
                throw new TypeError(`Not supported synchronization direction type when attempting. Given type: ${this.direction}`);
        }

        new command(this.fs, this.activity, start, this.pageSize).execute()
            .done((x: filesystemListPageDto<synchronizationDetail>) => {
                this.totalCount(x.TotalCount);
                this.items.removeAll();

                for (var i = 0; i < x.Items.length; i++) {
                    this.items.push(x.Items[i]);
                }
            });
    }
}

export = activityItems;