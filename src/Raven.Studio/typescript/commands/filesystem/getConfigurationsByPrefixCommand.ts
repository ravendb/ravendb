import commandBase = require("commands/commandBase");
import filesystem = require("models/filesystem/filesystem");

class getConfigurationNamesByPrefixCommand extends commandBase {

    constructor(private fs: filesystem, private prefix: string) {
        super();
    }

    execute(): JQueryPromise<configurationSearchResultsDto> {

        var url = "/config/search";
        var args = {
            prefix: this.prefix
        };

        var task = $.Deferred();
        this.query<configurationSearchResultsDto>(url, args, this.fs)
            .done((data: configurationSearchResultsDto) => {
               
                task.resolve(data);
            })
            .fail(response => this.reportError(`Failed to search configuration keys for prefix: ${this.prefix}`, response.responseText, response.statusText));

        return task;
    }
}

export = getConfigurationNamesByPrefixCommand; 