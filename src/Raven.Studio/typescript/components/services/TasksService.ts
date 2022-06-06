/// <reference path="../../../typings/tsd.d.ts" />

import database from "models/resources/database";
import getOngoingTasksCommand from "commands/database/tasks/getOngoingTasksCommand";
import deleteOngoingTaskCommand from "commands/database/tasks/deleteOngoingTaskCommand";
import toggleOngoingTaskCommand from "commands/database/tasks/toggleOngoingTaskCommand";

export default class TasksService {
    async getOngoingTasks(db: database, location: databaseLocationSpecifier) {
        return new getOngoingTasksCommand(db, location).execute();
    }

    async deleteOngoingTask(
        db: database,
        taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
        taskId: number,
        taskName: string
    ) {
        return new deleteOngoingTaskCommand(db, taskType, taskId, taskName).execute();
    }

    async toggleOngoingTask(
        db: database,
        taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
        taskId: number,
        taskName: string,
        enable: boolean
    ) {
        return new toggleOngoingTaskCommand(db, taskType, taskId, taskName, !enable).execute();
    }
}
