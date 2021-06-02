/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import abstractOngoingTaskEtlListModel = require("models/database/tasks/abstractOngoingTaskEtlListModel");
import appUrl = require("common/appUrl");

class ongoingTaskRavenEtlListModel extends abstractOngoingTaskEtlListModel {
    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    topologyDiscoveryUrls = ko.observableArray<string>([]);

    connectionStringDefined: KnockoutComputed<boolean>;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.connectionStringsUrl = appUrl.forConnectionStrings(activeDatabaseTracker.default.database(), "raven", this.connectionStringName());
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRavenEtl(this.taskId);

        this.connectionStringDefined = ko.pureComputed(() => !!this.destinationDB());
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || 'N/A');
        this.connectionStringName(dto.ConnectionStringName);
        this.topologyDiscoveryUrls(dto.TopologyDiscoveryUrls);
    }

}

export = ongoingTaskRavenEtlListModel;
