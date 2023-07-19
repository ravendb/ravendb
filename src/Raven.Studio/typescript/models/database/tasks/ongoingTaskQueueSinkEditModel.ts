/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import jsonUtil = require("common/jsonUtil");
import ongoingTaskQueueSinkScriptModel from "models/database/tasks/ongoingTaskQueueSinkScriptModel";

abstract class ongoingTaskQueueSinkEditModel extends ongoingTaskEditModel {
    
    connectionStringName = ko.observable<string>();
        
    scripts = ko.observableArray<ongoingTaskQueueSinkScriptModel>([]);

    showEditScriptArea: KnockoutComputed<boolean>;

    scriptSelectedForEdit = ko.observable<ongoingTaskQueueSinkScriptModel>();
    editedScriptSandbox = ko.observable<ongoingTaskQueueSinkScriptModel>();
    
    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.showEditScriptArea = ko.pureComputed(() => !!this.editedScriptSandbox());
        
        const innerDirtyFlag = ko.pureComputed(() => !!this.editedScriptSandbox() && this.editedScriptSandbox().dirtyFlag().isDirty());
        const scriptsCount = ko.pureComputed(() => this.scripts().length);
        const hasAnyDirtyScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.scripts().forEach(script => {
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
                this.taskState,
                this.mentorNode,
                this.pinMentorNode,
                this.manualChooseMentor,
                this.connectionStringName,
                scriptsCount,
                hasAnyDirtyScript
            ],
            false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });
        
        this.scripts.extend({
            validation: [
                {
                    validator: () => this.scripts().length > 0,
                    message: "Script is Not defined"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            mentorNode: this.mentorNode,
            scripts: this.scripts,
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink) {
        super.update(dto);
        const configuration = dto.Configuration;
        
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.scripts(configuration.Scripts.map(x => new ongoingTaskQueueSinkScriptModel(x, false)));
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
        }
    }

    protected toDto(broker: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType): Raven.Client.Documents.Operations.QueueSink.QueueSinkConfiguration {
        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            Disabled: this.taskState() === "Disabled",
            Scripts: this.scripts().map(x => x.toDto()),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            TaskId: this.taskId,
            BrokerType: broker,
        };
    }

    deleteScript(script: ongoingTaskQueueSinkScriptModel) { 
        this.scripts.remove(x => script.name() === x.name());
        
        if (this.scriptSelectedForEdit() === script) {
            this.editedScriptSandbox(null);
            this.scriptSelectedForEdit(null);
        }
    }

    editScript(script: ongoingTaskQueueSinkScriptModel) {
        this.scriptSelectedForEdit(script);
        this.editedScriptSandbox(new ongoingTaskQueueSinkScriptModel(script.toDto(), false));
    }
}

export = ongoingTaskQueueSinkEditModel;
