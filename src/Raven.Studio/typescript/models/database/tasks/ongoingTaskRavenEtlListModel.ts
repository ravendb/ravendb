/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class ongoingTaskRavenEtlListModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;
    connectionStringsUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    showRavenEtlDetails = ko.observable(false);

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlListView) {
        super();

        this.isInTasksListView = true;
        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRavenEtl(this.taskId);
        this.connectionStringsUrl = urls.connectionStrings;
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlListView) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
        this.connectionStringName(dto.ConnectionStringName);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showRavenEtlDetails(!this.showRavenEtlDetails());
    }

}

export = ongoingTaskRavenEtlListModel;
