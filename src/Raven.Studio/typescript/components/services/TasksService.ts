/// <reference path="../../../typings/tsd.d.ts" />

import database from "models/resources/database";
import getOngoingTasksCommand from "commands/database/tasks/getOngoingTasksCommand";
import deleteOngoingTaskCommand from "commands/database/tasks/deleteOngoingTaskCommand";
import toggleOngoingTaskCommand from "commands/database/tasks/toggleOngoingTaskCommand";
import etlProgressCommand from "commands/database/tasks/etlProgressCommand";
import { OngoingTaskSharedInfo } from "../models/tasks";
import TaskUtils from "../utils/TaskUtils";
import getManualBackupCommand from "commands/database/tasks/getManualBackupCommand";

export default class TasksService {
    async getOngoingTasks(db: database, location: databaseLocationSpecifier) {
        return new getOngoingTasksCommand(db, location).execute();
    }

    async deleteOngoingTask(db: database, task: OngoingTaskSharedInfo) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new deleteOngoingTaskCommand(db, taskType, task.taskId, task.taskName).execute();
    }

    async toggleOngoingTask(db: database, task: OngoingTaskSharedInfo, enable: boolean) {
        const taskType = TaskUtils.studioTaskTypeToTaskType(task.taskType);
        return new toggleOngoingTaskCommand(db, taskType, task.taskId, task.taskName, !enable).execute();
    }

    async getProgress(db: database, location: databaseLocationSpecifier) {
        return new etlProgressCommand(db, location, false).execute();
    }

    async getManualBackup(db: database) {
        return new getManualBackupCommand(db.name).execute();
    }
}
