/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskRavenEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskRavenEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    transformationScripts = ko.observableArray<ongoingTaskRavenEtlTransformationModel>([]);

    showEditTransformationArea = ko.observable<boolean>(false);

    editedTransformationScript = ko.observable<ongoingTaskRavenEtlTransformationModel>(ongoingTaskRavenEtlTransformationModel.empty());  
    isDirtyEditedScript = new ko.DirtyFlag([]);
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.initializeMentorValidation();

        this.isDirtyEditedScript = new ko.DirtyFlag([this.editedTransformationScript().name,
                                                        this.editedTransformationScript().script,
                                                        this.editedTransformationScript().transformScriptCollections],
                                                        false, jsonUtil.newLineNormalizingHashFunction);
        
    }
    
    private initValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });
        
        this.transformationScripts.extend({
            validation: [
                {
                    validator: () => this.transformationScripts().length > 0,
                    message: "Transformation Script is Not defined"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            preferredMentor: this.preferredMentor,
            transformationScripts: this.transformationScripts
        });
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskRavenEtlTransformationModel(x, false)));
            this.manualChooseMentor(!!dto.Configuration.MentorNode);
            this.preferredMentor(dto.Configuration.MentorNode);
        }
    }

    toDto(): Raven.Client.ServerWide.ETL.RavenEtlConfiguration { 
        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: false,
            Transforms: this.transformationScripts().map(x => x.toDto()),
            EtlType: "Raven",
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            TaskId: this.taskId,
        } as Raven.Client.ServerWide.ETL.RavenEtlConfiguration;
    }

    deleteTransformationScript(transformationScript: ongoingTaskRavenEtlTransformationModel) { 
        this.transformationScripts.remove(x => transformationScript.name() === x.name());
        this.showEditTransformationArea(false);
    }

    editTransformationScript(transformationScript: ongoingTaskRavenEtlTransformationModel) {
        this.editedTransformationScript().update(transformationScript.toDto(), false);
        this.showEditTransformationArea(true);
        this.isDirtyEditedScript().reset();
    }

    static empty(): ongoingTaskRavenEtlEditModel {
        return new ongoingTaskRavenEtlEditModel(
            {
                TaskName: "",
                TaskType: "RavenEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    EtlType: "Raven",
                    Transforms: [],
                    ConnectionStringName: "",
                    Name: "",
                },
            } as Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlDetails);
    }
}

export = ongoingTaskRavenEtlEditModel;
