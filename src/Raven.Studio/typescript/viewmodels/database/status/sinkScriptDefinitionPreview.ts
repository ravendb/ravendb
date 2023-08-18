import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;

class sinkScriptDefinitionPreview extends dialogViewModelBase {

    view = require("views/database/status/sinkScriptDefinitionPreview.html");
    
    taskName = ko.observable<string>();
    scriptName = ko.observable<string>();
    
    sinkScript = ko.observable<Raven.Client.Documents.Operations.QueueSink.QueueSinkScript>();
    
    script = ko.observable<string>();
    
    spinners = {
        loading: ko.observable<boolean>(true)
    };
    
    constructor(scriptName: string,
                task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink>) {
        super();

        aceEditorBindingHandler.install();

        task.done(result => {
            const matchedScript = result.Configuration.Scripts.find(x => x.Name === scriptName);
            if (matchedScript) {
                
                this.taskName(result.TaskName);
                this.scriptName(matchedScript.Name);
                this.sinkScript(matchedScript);
                
                if (matchedScript.Script) {
                    this.script(matchedScript.Script);
                }
            }
        })
            .always(() => {
                this.spinners.loading(false);
            });
    }
}

export = sinkScriptDefinitionPreview;
