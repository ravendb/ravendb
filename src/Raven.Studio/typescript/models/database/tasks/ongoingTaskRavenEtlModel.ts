/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import ongoingTaskEtlTransformationModel = require("models/database/tasks/ongoingTaskEtlTransformationModel");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskRavenEtlModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;
    connectionStringsUrl: KnockoutComputed<string>;
    protected activeDatabase = activeDatabaseTracker.default.database;

    destinationDB = ko.observable<string>();
    destinationURL = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    transformationScripts = ko.observableArray<ongoingTaskEtlTransformationModel>([]);

    showRavenEtlDetails = ko.observable(false); // used in tasks list view
    showEditTransformationArea = ko.observable<boolean>(false); // used for edit transfromation

    editedTransformationScript = ko.observable<ongoingTaskEtlTransformationModel>(ongoingTaskEtlTransformationModel.empty());
    isDirtyEditedScript = new ko.DirtyFlag([]);

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editRavenEtl(this.taskId);
        this.connectionStringsUrl = urls.connectionStrings;
       
        this.isDirtyEditedScript = new ko.DirtyFlag([this.editedTransformationScript().name,
                                                        this.editedTransformationScript().script,
                                                        this.editedTransformationScript().transformScriptCollections],
                                                        false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl) {
        super.update(dto);

        // Data for the model in List View
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl);
        this.connectionStringName(dto.ConnectionStringName);

        // Data for the model in Edit View (Note: dto.configuration is only available for Edit View)
        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskEtlTransformationModel(x, false)));
        }
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showRavenEtlDetails(!this.showRavenEtlDetails());
    }

    toDto(): ravenEtlDataFromUI {
        const Transformations = this.transformationScripts().map(x => {
            const collections = x.applyScriptForAllCollections() ? null : x.transformScriptCollections();
            return {
                ApplyToAllDocuments: x.applyScriptForAllCollections(),
                Collections: collections,
                Disabled: false,
                HasLoadAttachment: false,
                Name: x.name(),
                Script: x.script()
            };
        });

        return {
            TaskName: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            TransformationScripts: Transformations
        };
    }

    deleteTransformationScript(transformationScript: ongoingTaskEtlTransformationModel) {
        this.transformationScripts.remove(x => transformationScript.name() === x.name());
        this.showEditTransformationArea(false);
    }

    editTransformationScript(transformationScript: ongoingTaskEtlTransformationModel) {
        this.editedTransformationScript().update(transformationScript.toDto(), false);
        this.showEditTransformationArea(true);
        this.isDirtyEditedScript().reset();
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
                    Transforms: [],
                    AllowEtlOnNonEncryptedChannel: false,
                    ConnectionStringName: "",
                    Disabled: false,
                    Name: "",
                    TaskId: null,
                    MentorNode: null
                },
                DestinationDatabase: "",
                DestinationUrl: "",
                ConnectionStringName: "",
                Error: null
            }, false);
    }
}

export = ongoingTaskRavenEtlModel;
