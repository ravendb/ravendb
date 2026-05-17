import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

type TaskCategory = "Etl" | "Ai";

interface deleteEtlErrorsDto {
    type: TaskCategory;
    name: string[];
    shardNumber?: number
    nodeTag?: string
}

class deleteEtlErrorsCommand extends commandBase {
    constructor(private db: database | string, private deleteEtlDto: deleteEtlErrorsDto) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.taskErrors.tasksErrors + this.urlEncodeArgs(this.deleteEtlDto);

        return this.del<void>(url, null, this.db, { dataType: "text" });
    }
}

export = deleteEtlErrorsCommand;
