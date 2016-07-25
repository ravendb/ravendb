import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");
import customColumns = require("models/database/documents/customColumns");
import customColumnParams = require('models/database/documents/customColumnParams');
import pagedList = require("common/pagedList");
import getDocRefsCommand = require("commands/database/debug/getDocRefsCommand");


class statusDebugDocrefs extends viewModelBase {

    currentDocRefs = ko.observable<pagedList>();
    columns = ko.observable(customColumns.empty());
    docId = ko.observable<string>("");
    canSearch = ko.computed(() => this.docId().length > 0);
    
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
        this.currentDocRefs(this.createPagedList(this.activeDatabase()));
    }

    private createPagedList(db: database) : pagedList {
        var fetcher = (skip: number, take: number) => {
            return new getDocRefsCommand(db, this.docId(), skip, take).execute();
        }

        return new pagedList(fetcher);
    }
}

export = statusDebugDocrefs;
