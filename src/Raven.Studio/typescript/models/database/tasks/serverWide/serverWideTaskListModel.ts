/// <reference path="../../../../../typings/tsd.d.ts"/>
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");

//TODO remove
abstract class serverWideTaskListModel extends ongoingTaskListModel {

    excludedDatabases = ko.observableArray<string>();
}

export = serverWideTaskListModel;
