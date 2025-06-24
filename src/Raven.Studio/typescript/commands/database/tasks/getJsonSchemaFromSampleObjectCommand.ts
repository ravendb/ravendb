import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getJsonSchemaFromSampleObjectCommand extends commandBase {
    constructor(private payload: object) {
        super();
    }

    execute(): JQueryPromise<{ Result: string }> {
        const url = endpoints.global.studioTasks.studioTasksConvertToJsonSchema;

        return this.post<{ Result: string }>(url, JSON.stringify(this.payload)).fail((response: JQueryXHR) => {
            this.reportError(
                `Failed to get JSON schema from sample object`,
                response.responseText,
                response.statusText
            );
        });
    }
}

export = getJsonSchemaFromSampleObjectCommand;
