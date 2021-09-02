/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskElasticSearchEtlListModel extends abstractOngoingTaskEtlListModel {
    nodesUrls = ko.observableArray<string>([]);

    //connectionStringDefined: KnockoutComputed<boolean>; // connection string ??
    connectionStringDefined = ko.observable<boolean>(true); // needed for template in the ongoing tasks list view .. todo ???

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "elasticSearch", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editElasticSearchEtl(this.taskId);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
        this.nodesUrls(dto.NodesUrls);
    }
}

export = ongoingTaskElasticSearchEtlListModel;
