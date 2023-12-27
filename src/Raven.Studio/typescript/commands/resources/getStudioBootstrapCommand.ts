import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getStudioBootstrapCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration> {
        const url = endpoints.global.studioTasks.studioTasksBootstrap;
        
        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.StudioBootstrapConfiguration>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to load the studio configuration`, response.responseText, response.statusText);
            });
    }
}

export = getStudioBootstrapCommand;
