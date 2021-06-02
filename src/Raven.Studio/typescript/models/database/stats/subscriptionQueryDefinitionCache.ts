/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import app = require("durandal/app");
import subscriptionQueryDefinitionPreview = require("viewmodels/database/status/subscriptionQueryDefinitionPreview");

class subscriptionQueryDefinitionCache {
    private readonly db: database;

    constructor(db: database) {
        this.db = db;
    }

    showDefinitionFor(taskId: number, taskName: string) {
        const command = getOngoingTaskInfoCommand.forSubscription(this.db, taskId, taskName);

        const task = command.execute();

        const dialog = new subscriptionQueryDefinitionPreview(task);
        app.showBootstrapDialog(dialog);
    }
}

export = subscriptionQueryDefinitionCache;
