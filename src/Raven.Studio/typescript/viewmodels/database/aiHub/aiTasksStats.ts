import database = require("models/resources/database");
import ongoingTasksStats = require("../status/ongoingTasksStats");

const filteredTaskTypes: FilterOngoingTaskType[] = ["EmbeddingsGeneration"];

class aiTasksStats extends ongoingTasksStats {
    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location, filteredTaskTypes);
    }
}

export = aiTasksStats;
