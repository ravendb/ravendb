/// <reference path="../../../../typings/tsd.d.ts"/>
import database = require("models/resources/database");
import getOngoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import app = require("durandal/app");
import etlScriptDefinitionPreview = require("viewmodels/database/status/etlScriptDefinitionPreview");
import genUtils from "common/generalUtils";
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;

class etlScriptDefinitionCache {
    private readonly taskInfoCache = new Map<number, etlScriptDefinitionCacheItem>();
    private readonly db: database | string;

    constructor(db: database | string) {
        this.db = db;
    }

    showDefinitionFor(etlType: EtlType, taskId: number, transformationName: string) {
        let cachedItem = this.taskInfoCache.get(taskId);

        if (!cachedItem || cachedItem.task.state() === "rejected") {
            // cache item is missing or it failed

            let command: getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl |
                                                   Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl>;

            const databaseName = (typeof this.db === "string" ? this.db : this.db.name);
            
            switch (etlType) {
                case "Raven":
                    command = getOngoingTaskInfoCommand.forRavenEtl(databaseName, taskId);
                    break;
                case "Sql":
                    command = getOngoingTaskInfoCommand.forSqlEtl(databaseName, taskId);
                    break;
                case "Olap":
                    command = getOngoingTaskInfoCommand.forOlapEtl(databaseName, taskId);
                    break;
                case "ElasticSearch":
                    command = getOngoingTaskInfoCommand.forElasticSearchEtl(databaseName, taskId);
                    break;
                case "Queue":
                    command = getOngoingTaskInfoCommand.forQueueEtl(databaseName, taskId);
                    break;
                default: 
                    genUtils.assertUnreachable(etlType, "Unknown studioEtlType: " + etlType);
            }

            cachedItem = {
                etlType: etlType,
                task: command.execute()
            };

            this.taskInfoCache.set(taskId, cachedItem);
        }

        const dialog = new etlScriptDefinitionPreview(etlType, transformationName, cachedItem.task);
        app.showBootstrapDialog(dialog);
    }
}

export = etlScriptDefinitionCache;
