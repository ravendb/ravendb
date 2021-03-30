/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import app = require("durandal/app");
import etlScriptDefinitionPreview = require("viewmodels/database/status/etlScriptDefinitionPreview");

class etlScriptDefinitionCache {
    private readonly taskInfoCache = new Map<number, etlScriptDefinitionCacheItem>();
    private readonly db: database;

    constructor(db: database) {
        this.db = db;
    }

    showDefinitionFor(etlType: Raven.Client.Documents.Operations.ETL.EtlType, taskId: number, transformationName: string) {
        let cachedItem = this.taskInfoCache.get(taskId);

        if (!cachedItem || cachedItem.task.state() === "rejected") {
            // cache item is missing or it failed

            let command: getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails>;
            switch (etlType) {
                case "Raven":
                    command = getOngoingTaskInfoCommand.forRavenEtl(this.db, taskId);
                    break;
                case "Sql":
                    command = getOngoingTaskInfoCommand.forSqlEtl(this.db, taskId);
                    break;
                case "Olap":
                    command = getOngoingTaskInfoCommand.forOlapEtl(this.db, taskId);
                    break;
            }

            cachedItem = {
                etlType: etlType,
                task: command.execute()
            };

            this.taskInfoCache.set(taskId, cachedItem);
        }

        const dialog = new etlScriptDefinitionPreview(cachedItem.etlType, transformationName, cachedItem.task);
        app.showBootstrapDialog(dialog);
    }
}

export = etlScriptDefinitionCache;
