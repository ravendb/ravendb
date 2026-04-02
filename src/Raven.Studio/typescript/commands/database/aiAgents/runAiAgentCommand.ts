import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import aiAgentsTypes = require("components/pages/database/aiHub/aiAgents/utils/aiAgentsTypes");
import { ChatAiAgentAttachment } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";

export interface RunAiAgentRequestDto
    extends Omit<Raven.Client.Documents.Operations.AI.Agents.ConversionRequestBody, "UserPrompt"> {
    UserPrompt: string | { type: "text"; text: string }[];
    attachments?: ChatAiAgentAttachment[];
}

interface AttachmentPutCommandBase {
    Id: string;
    Name: string;
    ContentType: string;
    ChangeVector: string;
    FromEtl: boolean;
}

interface AttachmentPutCommandLocalFile extends AttachmentPutCommandBase {
    Type: "AttachmentPUT";
}

interface AttachmentPutCommandDocumentAttachment extends AttachmentPutCommandBase {
    Type: "AttachmentCOPY";
    DestinationId: string;
    DestinationName: string;
}

type AttachmentPutCommandDto = AttachmentPutCommandLocalFile | AttachmentPutCommandDocumentAttachment;

export default class runAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: RunAiAgentRequestDto,
        private agentId: string,
        private conversationId: string,
        private changeVector: string
    ) {
        super();
    }

    execute(): JQueryPromise<aiAgentsTypes.AiAgentRunResult> {
        const args = {
            agentId: this.agentId,
            conversationId: this.conversationId,
            changeVector: this.changeVector,
        };

        const url = endpoints.databases.aiAgent.aiAgent + this.urlEncodeArgs(args);
        const requestPayload = this.createRequestPayload();
        const requestOptions = this.createRequestOptions();

        return this.post(url, requestPayload, this.db, requestOptions).fail((response: JQueryXHR) =>
            this.reportError("Failed to run AI agent", response.responseText, response.statusText)
        );
    }

    private createRequestPayload(): string | FormData {
        if (this.dto.attachments?.length > 0) {
            return this.createMultipartPayload(this.dto.attachments);
        }

        return JSON.stringify(this.createRequestBody());
    }

    private createRequestOptions(): JQueryAjaxSettings {
        if (this.dto.attachments?.length > 0) {
            return {
                processData: false,
                contentType: false,
                cache: false,
                dataType: "json",
            };
        }

        return undefined;
    }

    private createMultipartPayload(attachments: ChatAiAgentAttachment[]): FormData {
        const formData = new FormData();
        const attachmentCommands = attachments.map(this.createAttachmentCommand);

        formData.append(
            "request",
            new Blob([JSON.stringify(this.createRequestBody())], {
                type: "application/json",
            })
        );

        formData.append(
            "commands",
            new Blob(
                [
                    JSON.stringify({
                        Commands: attachmentCommands,
                    }),
                ],
                {
                    type: "application/json",
                }
            )
        );

        attachments
            .filter((x) => x.type === "localFile")
            .forEach((attachment) => {
                formData.append("attachment", attachment.file, attachment.file.name);
            });

        return formData;
    }

    private createAttachmentCommand(attachment: ChatAiAgentAttachment): AttachmentPutCommandDto {
        if (attachment.type === "localFile") {
            return {
                Type: "AttachmentPUT",
                Id: "__this__",
                Name: attachment.name,
                ContentType: attachment.contentType,
                ChangeVector: null,
                FromEtl: false,
            };
        }

        return {
            Type: "AttachmentCOPY",
            Id: attachment.sourceDocumentId,
            Name: attachment.originalName,
            ContentType: attachment.contentType,
            ChangeVector: null,
            FromEtl: false,
            DestinationId: "__this__",
            DestinationName: attachment.name,
        };
    }

    private createRequestBody(): Omit<RunAiAgentRequestDto, "attachments"> {
        const dto = { ...this.dto };
        delete dto.attachments;
        return dto;
    }
}
