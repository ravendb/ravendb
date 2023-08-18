/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import app = require("durandal/app");
import sinkScriptDefinitionPreview = require("viewmodels/database/status/sinkScriptDefinitionPreview");

class sinkScriptDefinitionCache {
    private readonly taskInfoCache = new Map<number, sinkScriptDefinitionCacheItem>();
    private readonly db: database;

    constructor(db: database) {
        this.db = db;
    }

    showDefinitionFor(taskId: number, transformationName: string) {
        let cachedItem = this.taskInfoCache.get(taskId);

        if (!cachedItem || cachedItem.task.state() === "rejected") {
            // cache item is missing or it failed

            const command = getOngoingTaskInfoCommand.forQueueSink(this.db, taskId);

            cachedItem = {
                task: command.execute()
            };

            this.taskInfoCache.set(taskId, cachedItem);
        }

        const dialog = new sinkScriptDefinitionPreview(transformationName, cachedItem.task);
        app.showBootstrapDialog(dialog);
    }
}

export = sinkScriptDefinitionCache;
