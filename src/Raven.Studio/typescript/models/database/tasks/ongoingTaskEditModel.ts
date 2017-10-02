/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");

abstract class ongoingTaskEditModel extends ongoingTaskModel {
    manualChooseMentor = ko.observable<boolean>(false);
    preferredMentor = ko.observable<string>();
    
    protected update(dto: Raven.Client.ServerWide.Operations.OngoingTask) {
        super.update(dto);
        
        this.taskName(dto.TaskName);
    }
    
    protected initializeMentorValidation() {
        this.preferredMentor.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
    }
}

export = ongoingTaskEditModel;
