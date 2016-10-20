import commandBase = require("commands/commandBase");
import resource = require("models/resources/resource");
import endpoints = require("endpoints");

type rawDisableResourceResult = {
    name: string;
    success: boolean;
    reason: string;
    disabled: boolean;
}

class disableResourceToggleCommand extends commandBase {

    constructor(private resources: Array<resource>, private disable: boolean) {
        super();
    }

    get action() {
        return this.disable ? "disable" : "enable";
    }

    execute(): JQueryPromise<Array<disableResourceResult>> {
        const tasks = [] as Array<Promise<Array<disableResourceResult>>>;

        disableResourceToggleCommand
            .groupResourcesByQualifier(this.resources)
            .forEach((resources, group) => {
                tasks.push(Promise.resolve(this.toggleDisableForGroup(group, resources)));
            });

        const joinedResultTask = $.Deferred<Array<disableResourceResult>>();

        Promise.all(tasks)
            .then((results: Array<Array<disableResourceResult>>) => {
                const joinedResult = ([]).concat.apply([], results);
                joinedResultTask.resolve(joinedResult);
            })
            .catch((result: any) => joinedResultTask.reject(result));

        return joinedResultTask;
    }

    private static groupResourcesByQualifier(resources: Array<resource>) {
        const result = new Map<string, Array<resource>>();

        resources.forEach(rs => {
            const qualifier = rs.qualifier;

            if (result.has(qualifier)) {
                result.get(qualifier).push(rs);
            } else {
                result.set(qualifier, [rs]);
            }
        });

        return result;
    }

    private toggleDisableForGroup(qualifer: string, resources: resource[]): JQueryPromise<Array<disableResourceResult>> {
        const args = {
            name: resources.map(x => x.name),
            disable: this.disable
        };

        const url = "/admin/" +
            resources[0].urlPrefix +
            endpoints.admin.adminResourcesStudioTasks.toggleDisable +
            this.urlEncodeArgs(args);

        const task = $.Deferred<Array<disableResourceResult>>();

        this.post(url, null)
            .done(result => task.resolve(this.extractAndMapResult(qualifer, result)))
            .fail(reason => task.reject(reason));

        return task;
    }

    private extractAndMapResult(qualifer: string, result: Array<rawDisableResourceResult>): Array<disableResourceResult> {
        return result.map(x => ({
            qualifiedName: qualifer + "/" + x.name,
            success: x.success,
            reason: x.reason,
            disabled: x.disabled
        }));
    }

}

export = disableResourceToggleCommand;  
