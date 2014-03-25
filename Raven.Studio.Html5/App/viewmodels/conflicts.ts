import app = require("durandal/app");
import router = require("plugins/router");

import conflict = require("models/conflict");
import database = require("models/database");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getConflictsCommand = require("commands/getConflictsCommand");
import getReplicationSourcesCommand = require("commands/getReplicationSourcesCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import pagedResultSet = require("common/pagedResultSet");
import replicationSource = require("models/replicationSource");
import conflictVersion = require("models/conflictVersion");

class conflicts extends viewModelBase {

    displayName = "conflicts";
    sourcesLookup: { [s: string]: replicationSource; } = {};


    //TODO: cache for replication sources
    currentConflictsPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    static gridSelector = "#conflictsGrid";

    activate(args) {
        super.activate(args);
        this.activeDatabase.subscribe((db: database) => this.databaseChanged(db));
        this.fetchConflicts(appUrl.getDatabase());

        return this.loadReplicationSources(this.activeDatabase());
    }

    fetchConflicts(database: database) {
        this.currentConflictsPagedItems(this.createPagedList(database));
    }

    loadReplicationSources(db: database): JQueryPromise<replicationSource[]> {
        return new getReplicationSourcesCommand(db)
            .execute()
            .done(results => this.replicationSourcesLoaded(results, db));
    }

    replicationSourcesLoaded(sources: Array<replicationSource>, db: database) {
        this.sourcesLookup = {};
        $.map(sources, s => this.sourcesLookup[s.serverInstanceId] = s);
    }

    databaseChanged(db: database) {
        var conflictsUrl = appUrl.forConflicts(db);
        router.navigate(conflictsUrl, false);
        this.fetchConflicts(db);
        //TODO: update replication sources
    }

    private createPagedList(database: database): pagedList {
        var fetcher = (skip: number, take: number) => new getConflictsCommand(database, skip, take).execute();
        return new pagedList(fetcher);
    }

    getUrlForConflict(conflictVersion: conflictVersion) {
        return appUrl.forEditDoc(conflictVersion.id, null, 0, this.activeDatabase());
    }

    getTextForVersion(conflictVersion: conflictVersion) {
        var replicationSource = this.sourcesLookup[conflictVersion.sourceId];
        var text = "";
        if (replicationSource && replicationSource.name) {
            text = " (" + replicationSource.name + ")";
        }
        return text;
    }

    getServerUrlForVersion(conflictVersion: conflictVersion) {
        var replicationSource = this.sourcesLookup[conflictVersion.sourceId];
        var text = "";
        if (replicationSource && replicationSource.source) {
            text = replicationSource.source;
        }
        return text;
        
    }

}

export = conflicts;