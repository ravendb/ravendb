/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskRavenEtlTransformationModel = require("models/database/tasks/ongoingTaskRavenEtlTransformationModel");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskRavenEtlEditModel extends ongoingTaskEditModel {
    
    connectionStringName = ko.observable<string>(); // Contains list of discovery urls in the targeted cluster. The task communicates with these urls.
        
    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    transformationScripts = ko.observableArray<ongoingTaskRavenEtlTransformationModel>([]);

    showEditTransformationArea: KnockoutComputed<boolean>;

    transformationScriptSelectedForEdit = ko.observable<ongoingTaskRavenEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskRavenEtlTransformationModel>();
    
    validationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());
        
        const innerDirtyFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const scriptsCount = ko.pureComputed(() => this.transformationScripts().length);
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
                innerDirtyFlag,
                this.taskName,
                this.mentorNode,
                this.manualChooseMentor,
                this.connectionStringName,
                this.allowEtlOnNonEncryptedChannel,
                scriptsCount,
                hasAnyDirtyTransformationScript
            ],
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
            mentorNode: this.mentorNode,
            transformationScripts: this.transformationScripts
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskRavenEtlTransformationModel(x, false, false)));
            this.manualChooseMentor(!!dto.Configuration.MentorNode);
        }
    }

    toDto(): Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration { 
        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: false,
            Transforms: this.transformationScripts().map(x => x.toDto()),
            EtlType: "Raven",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            TaskId: this.taskId,
        } as Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration;
    }

    deleteTransformationScript(transformationScript: ongoingTaskRavenEtlTransformationModel) { 
        this.transformationScripts.remove(x => transformationScript.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === transformationScript) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(transformationScript: ongoingTaskRavenEtlTransformationModel) {
        this.transformationScriptSelectedForEdit(transformationScript);
        this.editedTransformationScriptSandbox(new ongoingTaskRavenEtlTransformationModel(transformationScript.toDto(), false, transformationScript.resetScript()));
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
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails);
    }
}

export = ongoingTaskRavenEtlEditModel;
