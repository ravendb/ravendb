import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");
import synchronizationConfig = require("models/filesystem/synchronizationConfig");

class getSynchronizationConfigCommand extends commandBase {

    constructor(private fs: filesystem) {
        super();        
    }

    execute(): JQueryPromise<any> {

        var url = "/config";
        var args = {
            name: "Raven/Synchronization/Config"
        };

        var task = $.Deferred<synchronizationConfig>();
        this.query<string>(url, args, this.fs)
            .done(data => {
                task.resolve(new synchronizationConfig(data));
            })
            .fail(response => {
                if (response.status !== 404) {
                    this.reportError("Failed to retrieve filesystem synchronization config.", response.responseText, response.statusText);
                    task.reject();

                } else {
                    task.resolve(synchronizationConfig.empty());
                }
            });
        

        return task;
    }
}

export = getSynchronizationConfigCommand; 