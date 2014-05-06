import app = require("durandal/app");
import router = require("plugins/router");

import pagedResultSet = require("common/pagedResultSet");
import appUrl = require("common/appUrl");
import pagedList = require("common/pagedList");

import conflict = require("models/conflict");
import indexPriority = require("models/indexPriority");
import database = require("models/database");
import conflictVersion = require("models/conflictVersion");
import transformer = require("models/transformer");
import indexDefinition = require("models/indexDefinition");
import customColumns = require("models/customColumns");
import customColumnParams = require('models/customColumnParams');

import getConflictsCommand = require("commands/getConflictsCommand");
import getReplicationSourcesCommand = require("commands/getReplicationSourcesCommand");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import getSingleTransformerCommand = require("commands/getSingleTransformerCommand");
import saveIndexDefinitionCommand = require("commands/saveIndexDefinitionCommand");
import saveTransformerCommand = require("commands/saveTransformerCommand");


import viewModelBase = require("viewmodels/viewModelBase");

class conflicts extends viewModelBase {

    displayName = "conflicts";
    sourcesLookup: dictionary<string> = {};

    currentColumnsParams = ko.observable(customColumns.empty());

    //TODO: subscribe to databases and remove item from list once user delete DB.
    static performedIndexChecks: Array<string> = [];

    static conflictsIndexName = "Raven/ConflictDocuments";
    static conflictsTransformerName = "Raven/ConflictDocumentsTransformer";


    //TODO: cache for replication sources
    currentConflictsPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    static gridSelector = "#conflictsGrid";

    activate(args) {
        super.activate(args);
        this.activeDatabase.subscribe((db: database) => this.databaseChanged(db));

        this.currentColumnsParams().columns([
            new customColumnParams({ Header: "Detected At (UTC)", Binding: "conflictDetectedAt", DefaultWidth: 300 }),
            new customColumnParams({ Header: "Versions", Binding: "versions", DefaultWidth: 400, Template: 'versions-template' }),
        ]);

        return this.performIndexCheck(this.activeDatabase()).then(() => {
            return this.loadReplicationSources(this.activeDatabase());
        }).done(() => {
                this.fetchConflicts(appUrl.getDatabase());
            });
    }

    fetchConflicts(database: database) {
        this.currentConflictsPagedItems(this.createPagedList(database));
    }

    loadReplicationSources(db: database): JQueryPromise<dictionary<string>> {
        return new getReplicationSourcesCommand(db)
            .execute()
            .done(results => this.replicationSourcesLoaded(results, db));
    }

    performIndexCheck(db: database): JQueryPromise<any> {

        // first look in cache
        if (conflicts.performedIndexChecks.contains(db.name)) {
            return $.Deferred<any>().resolve();
        }

        var performCheckTask = $.Deferred<any>();

        // perform index check against DB
        $.when(new getIndexDefinitionCommand(conflicts.conflictsIndexName, db).execute(),
            new getSingleTransformerCommand(conflicts.conflictsTransformerName, db).execute())
            .done(() => {
                conflicts.performedIndexChecks.push(db.name);
                performCheckTask.resolve();
            })
            .fail( 
            function () {
                //TODO: we should check exact result of getIndex/Transfomer commands (if it contains 404)
                var indexTask = new saveIndexDefinitionCommand(conflicts.getConflictsIndexDefinition(), indexPriority.normal, db).execute();
                var transformerTask = new saveTransformerCommand(conflicts.getConflictsTransformerDefinition(), db).execute();

                $.when(indexTask, transformerTask).done(function () {
                    conflicts.performedIndexChecks.push(db.name);
                    performCheckTask.resolve();
                }).fail(() => performCheckTask.reject() );
            });

        return performCheckTask;
    }

    static getConflictsIndexDefinition(): indexDefinitionDto {
        var indexDef = indexDefinition.empty();
        indexDef.name(conflicts.conflictsIndexName);
        indexDef.maps()[0](
        "from doc in docs \r\n" +
            " let id = doc[\"@metadata\"][\"@id\"] \r\n" + 
            " where doc[\"@metadata\"][\"Raven-Replication-Conflict\"] == true && (id.Length < 47 || !id.Substring(id.Length - 47).StartsWith(\"/conflicts/\", StringComparison.OrdinalIgnoreCase)) \r\n" + 
            " select new { ConflictDetectedAt = (DateTime)doc[\"@metadata\"][\"Last-Modified\"] }");

        return indexDef.toDto();
    }
    static getConflictsTransformerDefinition(): transformer {
        var transDef = transformer.empty();
        transDef.name(conflicts.conflictsTransformerName);
        transDef.transformResults("from result in results \r\n" +
            "                select new {  \r\n" +
	        "                    Id = result[\"__document_id\"], \r\n" +
	        "                    ConflictDetectedAt = result[\"@metadata\"].Value<DateTime>(\"Last-Modified\"),  \r\n" +
	        "                    EntityName = result[\"@metadata\"][\"Raven-Entity-Name\"], \r\n" +
	        "                    Versions = result.Conflicts.Select(versionId => { \r\n" +
            "                        var version = LoadDocument(versionId); \r\n" +
            "                 return new { Id = versionId, SourceId = version[\"@metadata\"][\"Raven-Replication-Source\"] }; \r\n" +
            "             }) \r\n" +
            "         }\r\n");
        return transDef;
    }
    

    replicationSourcesLoaded(sources: dictionary<string> , db: database) {
        this.sourcesLookup = sources;
    }

    databaseChanged(db: database) {
        var conflictsUrl = appUrl.forConflicts(db);
        router.navigate(conflictsUrl, false);
        this.performIndexCheck(db).then(() => {
            return this.loadReplicationSources(db);
        }).done(() => {
                this.fetchConflicts(db);
        });
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
        if (replicationSource) {
            text = " (" + replicationSource + ")";
        }
        return text;
    }

    getServerUrlForVersion(conflictVersion: conflictVersion) {
        return this.sourcesLookup[conflictVersion.sourceId] || "";
    }

}

export = conflicts;