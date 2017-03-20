import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugDocrefs extends viewModelBase {

    //TODO: use virtualGrid currentDocRefs = ko.observable<pagedList>();
    //TODO: columns = ko.observable(customColumns.empty());
    docId = ko.observable<string>("");
    canSearch = ko.computed(() => this.docId().length > 0);

    /* TODO
    resultsCount = ko.computed(() => {
        if (!this.currentDocRefs())
            return -1;

        return this.currentDocRefs().totalResultCount();
    });

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.columns().columns([
            new customColumnParams({ Header: "Id", Binding: "Id", DefaultWidth: 300 }),
        ]);
    }

    fetchDocRefs() {
        eventsCollector.default.reportEvent("docrefs", "fetch");
        this.currentDocRefs(this.createPagedList(this.activeDatabase()));
    }*/

    /* TODO
    private createPagedList(db: database) : pagedList {
        var fetcher = (skip: number, take: number) => {
            return new getDocRefsCommand(db, this.docId(), skip, take).execute();
        }

        return new pagedList(fetcher);
    }*/
}

export = statusDebugDocrefs;
