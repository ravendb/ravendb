import getStatusDebugQueriesCommand = require("commands/database/debug/getStatusDebugQueriesCommand");
import getKillQueryCommand = require("commands/database/query/getKillQueryCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import statusDebugQueriesGroup = require("models/database/debug/statusDebugQueriesGroup");
import statusDebugQueriesQuery = require("models/database/debug/statusDebugQueriesQuery");
import autoRefreshBindingHandler = require("common/bindingHelpers/autoRefreshBindingHandler");


class statusDebugQueries extends viewModelBase {
    data = ko.observableArray<statusDebugQueriesGroup>();

    constructor() {
        super();
        autoRefreshBindingHandler.install();
        aceEditorBindingHandler.install();
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchCurrentQueries());
        return this.fetchCurrentQueries();
    }

    fetchCurrentQueries(): JQueryPromise<statusDebugQueriesGroupDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugQueriesCommand(db)
                .execute()
                .done((results: statusDebugQueriesGroupDto[]) => this.onResultsLoaded(results));
        }

        return null;
    }

    onResultsLoaded(results: statusDebugQueriesGroupDto[]) {
        var currentGroups = $.map(this.data(), (group) => group.indexName);

        $.map(results, (dtoGroup) => {
            if (dtoGroup.Queries.length > 0) {
                var foundGroup = this.data().first((item) => item.indexName === dtoGroup.IndexName);
                if (foundGroup) {
                    currentGroups.remove(dtoGroup.IndexName);
                } else {
                    foundGroup = new statusDebugQueriesGroup(dtoGroup);
                    this.data.push(foundGroup);
                }
                this.updateGroup(foundGroup, dtoGroup);
            }
        });

        // remove empty and unused groups
        currentGroups.forEach(group => {
            var foundGroup = this.data.first((item) => item.indexName === group);
            if (foundGroup) {
                this.data.remove(foundGroup);
            }
        });
    }

    updateGroup(group: statusDebugQueriesGroup, dtoGroup: statusDebugQueriesGroupDto) {
        var currentQueryIds = $.map(group.queries(), (query) => query.queryId);

        $.map(dtoGroup.Queries, (dtoQuery) => {
            var foundQuery = group.queries.first((item) => item.queryId == dtoQuery.QueryId);
            if (foundQuery) {
                currentQueryIds.remove(foundQuery.queryId);
                foundQuery.duration(dtoQuery.Duration);
            } else {
                group.queries.push(new statusDebugQueriesQuery(dtoQuery));
            }
        });

        // remove unused queries
        currentQueryIds.forEach(query => {
            var foundQuery = group.queries.first((item) => item.queryId == query);
            if (foundQuery) {
                group.queries.remove(foundQuery);
            }
        });
    } 

    killQuery(queryId: number) {
        new getKillQueryCommand(this.activeDatabase(), queryId)
            .execute()
            .done(() => {
                // find and delete query from model
                this.data().forEach(group => {
                    var foundQuery = group.queries.first(q => q.queryId == queryId);
                    if (foundQuery) {
                        group.queries.remove(foundQuery);
                    }
                });
            });
    }
}

export = statusDebugQueries;
