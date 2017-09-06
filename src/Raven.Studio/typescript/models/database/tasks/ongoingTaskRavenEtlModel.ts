/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class ongoingTaskRavenEtlModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);

    // TODO: Add support for the collections scripts dictionary
    //transformScripts = ko.observableArray<>([]);

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRavenEtl(this.taskId);
    }

    initValidation() {
        this.connectionStringName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl) {
        super.update(dto);

        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
        this.connectionStringName(dto.Configuration.ConnectionStringName);
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        // TODO...
    }

    toDto(): ravenEtlDataFromUI {
        return {
            TaskName: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel()
            // todo:  add list of scripts...
        };
    }

    static empty(): ongoingTaskRavenEtlModel {
        return new ongoingTaskRavenEtlModel(
            {
                TaskId: null,
                TaskName: "",
                TaskType: "RavenEtl",
                TaskState: "Enabled",
                ResponsibleNode: null,
                TaskConnectionStatus: "Active",
                Configuration: {
                    EtlType: "Raven",
                    Transforms: null,
                    AllowEtlOnNonEncryptedChannel: false,
                    ConnectionStringName: "",
                    Disabled: false,
                    Name: "",
                    TaskId: null
                },
                DestinationDatabase: "",
                DestinationUrl: ""
            }, false);
    }
}

export = ongoingTaskRavenEtlModel;
