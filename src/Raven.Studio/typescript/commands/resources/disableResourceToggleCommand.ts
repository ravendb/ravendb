import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

type disableResourceResult = {
    name: string;
    success: boolean;
    reason: string;
}

class disableResourceToggleCommand extends commandBase {

    constructor(private resources: Array<resource>, private disable: boolean) {
        super();
    }

    get action() {
        return this.disable ? "disable" : "enable";
    }

    execute(): JQueryPromise<Array<disableResourceResult>> {
        const tasks = [] as Array<JQueryPromise<Array<disableResourceResult>>>;

        disableResourceToggleCommand
            .groupResourcesByType(this.resources)
            .forEach((resources, group) => {
                tasks.push(this.toggleDisableForGroup(group, resources));
            });

        const joinedResultTask = $.Deferred<Array<disableResourceResult>>();

        $.when.apply(null, tasks)
            .done((...results: disableResourceResult[][]) => {
                joinedResultTask.resolve([].concat.apply([], results));
            })
            .fail((result: any) => joinedResultTask.reject(result));

        return joinedResultTask;
    }

    private static groupResourcesByType(resources: Array<resource>) {
        const result = new Map<string, Array<resource>>();

        resources.forEach(rs => {
            const qualifier = rs.urlPrefix;

            if (result.has(qualifier)) {
                result.get(qualifier).push(rs);
            } else {
                result.set(qualifier, [rs]);
            }
        });

        return result;
    }

    private toggleDisableForGroup(groupName: string, resources: resource[]): JQueryPromise<Array<disableResourceResult>> {
        const args = {
            name: resources.map(x => x.name),
            isDisabled: this.disable
        };

        const url = "/admin/" +
            groupName +
            endpoints.admin.adminResourcesStudioTasks.toggleDisable +
            this.urlEncodeArgs(args);

        return this.post(url, null, null);
    }

}

export = disableResourceToggleCommand;  
