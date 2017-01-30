import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteResourceCommand extends commandBase {

    constructor(private resources: Array<resource>, private isHardDelete: boolean) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Server.Web.System.ResourceDeleteResult>> {       

        const singleResource = this.resources.length === 1;

        const resourcesByQualifier = _.groupBy(this.resources, x => x.qualifier);

        const tasks = Object.keys(resourcesByQualifier).map(qualifier => {
            const resourceGroup = resourcesByQualifier[qualifier];

            const url = this.getDeleteEndpointUrlForQualifier(qualifier);
            const args = {
                "hard-delete": this.isHardDelete, 
                name: resourceGroup.map(x => x.name)
            };

            return Promise.resolve(
                this.del<Raven.Server.Web.System.ResourceDeleteResult[]>(url + this.urlEncodeArgs(args), null, null, 9000 * this.resources.length)
            );
        });
       
        const result = $.Deferred<Array<Raven.Server.Web.System.ResourceDeleteResult>>();

        Promise.all(tasks)
            .then((results) => {      
                // Flatten results first, since it is an array of arrays (holding the differenent resources types) 
                const allResults: Raven.Server.Web.System.ResourceDeleteResult[] = _.flatten(results);                 
                result.resolve(allResults);
            })
            .catch((response: JQueryXHR) => {
                this.reportError("Failed to delete resources", response.responseText, response.statusText);
                result.reject(response);
            });

        return result;
    }

    private getDeleteEndpointUrlForQualifier(qualifier: string) {
        if (qualifier === database.qualifier) {
            return endpoints.global.adminDatabases.adminDatabases;
        }
        throw new Error("qualifer is not yet supported" + qualifier);
    }

} 

export = deleteResourceCommand;
