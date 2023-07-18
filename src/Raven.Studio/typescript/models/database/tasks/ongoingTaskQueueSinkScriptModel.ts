/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class ongoingTaskQueueSinkScriptModel {
    
    name = ko.observable<string>();
    script = ko.observable<string>();
    queues = ko.observableArray<string>([]);

    isNew = ko.observable<boolean>(true);
    inputQueue = ko.observable<string>();
    canAddQueue: KnockoutComputed<boolean>;

    documentIdPostfix = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.QueueSink.QueueSinkScript, isNew: boolean) {
        this.update(dto, isNew);
        this.initObservables();
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
                this.name,
                this.script,
                this.queues,
                this.documentIdPostfix
            ],
            false, jsonUtil.newLineNormalizingHashFunction);
    }

    static empty(name?: string): ongoingTaskQueueSinkScriptModel {
        return new ongoingTaskQueueSinkScriptModel(
            {
                Queues: [],
                Disabled: false,
                Name: name || "",
                Script: "",
            }, true);
    }

    toDto(): Raven.Client.Documents.Operations.QueueSink.QueueSinkScript {
        return {
            Queues: this.queues(),
            Disabled: false,
            Name: this.name(),
            Script: this.script(),
        }
    }

    private initObservables() {
        this.canAddQueue = ko.pureComputed(() => {
            const queueToAdd = this.inputQueue();
            return queueToAdd && !this.queues().find(x => x === queueToAdd);
        });
    }

    private initValidation() {
        this.name.extend({
            required: true
        });
        
        this.queues.extend({
            validation: [
                {
                    validator: () => this.queues().length > 0,
                    message: "At least one queue is required"
                }
            ]
        });
        
        this.script.extend({
            required: true,
            aceValidation: true
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            queues: this.queues,
            script: this.script
        });
    }

    removeQueue(queue: string) {
        this.queues.remove(queue);
    }

    addQueue() {
        this.addWithBlink(this.inputQueue());
    }
    
    addWithBlink(queue: string) {
        this.queues.unshift(queue);
       
        this.inputQueue("");
        
        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    private update(dto: Raven.Client.Documents.Operations.QueueSink.QueueSinkScript, isNew: boolean) {
        this.name(dto.Name);
        this.script(dto.Script);
        this.queues(dto.Queues || []);
        this.isNew(isNew);
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskQueueSinkScriptModel;
