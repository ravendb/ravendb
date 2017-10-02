/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskEtlTransformationModel = require("models/database/tasks/ongoingTaskEtlTransformationModel");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskRavenEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    transformationScripts = ko.observableArray<ongoingTaskEtlTransformationModel>([]);

    showEditTransformationArea = ko.observable<boolean>(false);

    editedTransformationScript = ko.observable<ongoingTaskEtlTransformationModel>(ongoingTaskEtlTransformationModel.empty());
    isDirtyEditedScript = new ko.DirtyFlag([]);

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails) {
        super();

        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        this.isDirtyEditedScript = new ko.DirtyFlag([this.editedTransformationScript().name,
                                                        this.editedTransformationScript().script,
                                                        this.editedTransformationScript().transformScriptCollections],
                                                        false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskEtlTransformationModel(x, false)));
        }
    }

    toDto(taskId: number): Raven.Client.ServerWide.ETL.RavenEtlConfiguration {
        const transformations = this.transformationScripts().map(x => {
            const collections = x.applyScriptForAllCollections() ? null : x.transformScriptCollections();
            return {
                ApplyToAllDocuments: x.applyScriptForAllCollections(),
                Collections: collections,
                Disabled: false,
                HasLoadAttachment: false,
                Name: x.name(),
                Script: x.script()
            } as Raven.Client.ServerWide.ETL.Transformation;
        });

        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: false,
            Transforms: transformations,
            EtlType: "Raven",
            MentorNode: null, //TODO:
            TaskId: taskId
        } as Raven.Client.ServerWide.ETL.RavenEtlConfiguration;
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

    static empty(): ongoingTaskRavenEtlEditModel {
        return new ongoingTaskRavenEtlEditModel(
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
                Error: null,
            });
    }
}

export = ongoingTaskRavenEtlEditModel;
