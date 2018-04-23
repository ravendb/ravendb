/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");

abstract class ongoingTaskListModel extends ongoingTaskModel {
    
    showDetails = ko.observable(false);
    
    abstract toggleDetails(): void;
    
}

export = ongoingTaskListModel;
