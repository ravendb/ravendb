import getStatusDebugTasksCommand = require("commands/database/debug/getStatusDebugTasksCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class statusDebugTasks extends viewModelBase {
    data = ko.observable<taskMetadataDto[]>();

    activate(args) {
        super.activate(args);
        this.updateHelpLink('JHZ574');
        this.activeDatabase.subscribe(() => this.fetchTasks());
        return this.fetchTasks();
    }

    fetchTasks(): JQueryPromise<taskMetadataDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugTasksCommand(db)
                .execute()
                .done((results: taskMetadataDto[]) => this.data(results));
        }

        return null;
    }
}

export = statusDebugTasks;