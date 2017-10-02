/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");

abstract class ongoingTaskEditModel extends ongoingTaskModel {
    manualChooseMentor = ko.observable<boolean>(false);
    preferredMentor = ko.observable<string>();

    protected initializeMentorValidation() {
        this.preferredMentor.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
    }
}

export = ongoingTaskEditModel;
