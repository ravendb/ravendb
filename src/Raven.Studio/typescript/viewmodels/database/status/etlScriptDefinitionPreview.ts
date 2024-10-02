import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;

class etlScriptDefinitionPreview extends dialogViewModelBase {

    view = require("views/database/status/etlScriptDefinitionPreview.html");
    
    taskName = ko.observable<string>();
    transformationName = ko.observable<string>();
    
    etlType = ko.observable<EtlType>();
    
    transformation = ko.observable<Raven.Client.Documents.Operations.ETL.Transformation>();
    
    script = ko.observable<string>();
    
    spinners = {
        loading: ko.observable<boolean>(true)
    };
    
    constructor(etlType: EtlType,
                transformationName: string,
                task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl>) {
        super();

        aceEditorBindingHandler.install();

        task.done(result => {
            const matchedTransformation = result.Configuration.Transforms.find(x => x.Name === transformationName);
            if (matchedTransformation) {
                
                this.taskName(result.TaskName);
                this.transformationName(matchedTransformation.Name);
                this.etlType(etlType);
                this.transformation(matchedTransformation);
                
                if (matchedTransformation.Script) {
                    this.script(matchedTransformation.Script);
                }
            }
        })
            .always(() => {
                this.spinners.loading(false);
            });
    }
}

export = etlScriptDefinitionPreview;
