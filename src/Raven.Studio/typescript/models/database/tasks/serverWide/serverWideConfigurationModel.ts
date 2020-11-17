/// <reference path="../../../../../typings/tsd.d.ts"/>
import serverWideExcludeModel = require("models/database/tasks/serverWide/serverWideExcludeModel");

abstract class serverWideConfigurationModel {
    taskId = ko.observable<number>();
    taskName = ko.observable<string>();
    disabled = ko.observable<boolean>();
    
    mentorNode = ko.observable<string>();
    manualChooseMentor = ko.observable<boolean>(false);
    
    excludeInfo = ko.observable<serverWideExcludeModel>();

    constructor(taskId: number, taskName: string, disabled: boolean, mentorNode: string, excludedDatabases: string[]) {
        this.taskId(taskId);
        this.taskName(taskName);
        this.mentorNode(mentorNode);
        this.disabled(disabled);

        this.manualChooseMentor(!!mentorNode);
        
        this.excludeInfo(new serverWideExcludeModel(excludedDatabases));

        this.mentorNode.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
    }
}

export = serverWideConfigurationModel;
