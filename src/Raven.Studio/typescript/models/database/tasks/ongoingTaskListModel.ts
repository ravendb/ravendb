/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import router = require("plugins/router");

abstract class ongoingTaskListModel extends ongoingTaskModel {
    
    showDetails = ko.observable(false);

    editUrl: KnockoutComputed<string>;

    isServerWide = ko.observable<boolean>(false);
    
    abstract toggleDetails(): void;

    editTask() {
        router.navigate(this.editUrl());
    }
}

export = ongoingTaskListModel;
