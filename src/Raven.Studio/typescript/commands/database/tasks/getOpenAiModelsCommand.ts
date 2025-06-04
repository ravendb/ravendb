import commandBase = require("commands/commandBase");
import yup = require("yup");

class getOpenAiModelsCommand extends commandBase {
    private readonly defaultEndpoint = "https://api.openai.com/v1/";

    constructor(readonly apiKey: string, readonly projectId: string, readonly organizationId: string, readonly endpoint: string) {
        super();
    }

    execute(): JQueryPromise<string[]> {
        const endpoint = this.getEndpoint();
        const url = `${endpoint}models`;
        
        const options: JQueryAjaxSettings = {
            headers: {
                Authorization: `Bearer ${this.apiKey}`,
                "OpenAI-Organization": this.organizationId || undefined,
                "OpenAI-Project": this.projectId || undefined,
            },
        };

        return this.query<string[]>(url, null, null, this.mapToResult, options);
    }

    private getEndpoint(): string {
        let endpoint = this.endpoint;
        if (!endpoint) {
            endpoint = this.defaultEndpoint;
        }
        if (!endpoint.endsWith("/")) {
            endpoint += "/";
        }

        return endpoint;
    }

    private mapToResult(result: unknown): string[] {
        const parsedResult = yup
            .object({
                data: yup.array(yup.object({ id: yup.string().required() })).required(),
            })
            .validateSync(result);

        return parsedResult.data.map((model) => model.id);
    }
}

export = getOpenAiModelsCommand;
