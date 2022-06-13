import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

class etlScriptDefinitionPreview extends dialogViewModelBase {

    view = require("views/database/status/etlScriptDefinitionPreview.html");
    
    taskName = ko.observable<string>();
    transformationName = ko.observable<string>();
    
    etlType = ko.observable<Raven.Client.Documents.Operations.ETL.EtlType | StudioEtlType>();
    
    transformation = ko.observable<Raven.Client.Documents.Operations.ETL.Transformation>();
    
    script = ko.observable<string>();
    
    spinners = {
        loading: ko.observable<boolean>(true)
    };
    
    // if we come here from tasks-list-view we know studio-etl-type
    // if we come from stats-view we only know server-etl-type
    constructor(etlType: Raven.Client.Documents.Operations.ETL.EtlType | StudioEtlType,
                transformationName: string,
                task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails |
                                    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails>) {
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
