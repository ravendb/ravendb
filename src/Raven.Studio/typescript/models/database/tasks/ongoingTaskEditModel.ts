/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");

abstract class ongoingTaskEditModel extends ongoingTaskModel {
    manualChooseMentor = ko.observable<boolean>(false);
    
    nodeTag: string = null;

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask) {
        super.update(dto);

        this.taskName(dto.TaskName);
    }
    
    protected initializeMentorValidation() {
        this.mentorNode.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
    }
}

export = ongoingTaskEditModel;
