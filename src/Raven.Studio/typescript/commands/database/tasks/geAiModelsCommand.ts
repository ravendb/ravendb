import commandBase = require("commands/commandBase");
import yup = require("yup");
import endpoints = require("endpoints");

class geAiModelsCommand extends commandBase {
    constructor(readonly dto: AiModelsRequestDto) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        const url = endpoints.global.studioTasks.studioTasksAiModels;

        return this.post<string[]>(url, JSON.stringify(this.dto), null).then(this.mapToModelsArray);
    }

    private mapToModelsArray(result: unknown): string[] {
        const parsedResult = yup
            .object({
                data: yup.array(yup.object({ id: yup.string().required() })).required(),
            })
            .validateSync(result);

        return parsedResult.data.map((model) => model.id);
    }
}

export = geAiModelsCommand;
