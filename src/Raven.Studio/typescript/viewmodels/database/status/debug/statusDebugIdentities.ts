import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugIdentities extends viewModelBase {

    /* TODO currentIdentities = ko.observable<pagedList>();
    //TODO: columns = ko.observable(customColumns.empty());

    resultsCount = ko.computed(() => {
        if (!this.currentIdentities())
            return -1;

        return this.currentIdentities().totalResultCount();
    });

    activate(args: any) {
        super.activate(args);
        this.columns().columns([
            new customColumnParams({ Header: "Key", Binding: "Key", DefaultWidth: 300 }),
            new customColumnParams({ Header: "Value", Binding: "Value", DefaultWidth: 300 }),
        ]);
        this.updateHelpLink('JHZ574');
        this.fetchIdentities();
    }

    fetchIdentities() {
        this.currentIdentities(this.createPagedList(this.activeDatabase()));
    }

    private createPagedList(db: database): pagedList {
        var fetcher = (skip: number, take: number) => {
            return new getIdentitiesCommand(db, skip, take).execute();
        }
        return new pagedList(fetcher);
    }*/
}

export = statusDebugIdentities;
