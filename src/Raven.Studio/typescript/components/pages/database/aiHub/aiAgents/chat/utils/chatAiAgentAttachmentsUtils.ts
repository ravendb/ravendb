import genUtils from "common/generalUtils";
import messagePublisher from "common/messagePublisher";
import { ChatAiAgentAttachment } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";
import IconName from "typings/server/icons";

const validExtensions = ["txt", "pdf", "jpg", "jpeg", "png", "gif", "webp"];
const validExtensionsAccept = validExtensions.map((extension) => `.${extension}`).join(",");

interface AttachmentValidationErrors {
    invalidFiles?: string[];
    duplicateFiles?: string[];
}

function prepareLocalFiles(
    files: File[],
    existingAttachments: Pick<ChatAiAgentAttachment, "name">[]
): {
    attachments: ChatAiAgentAttachment[];
    invalidFiles: string[];
    duplicateFiles: string[];
} {
    const existingNames = new Set(existingAttachments.map((attachment) => attachment.name));
    const addedNames = new Set<string>();

    const invalidFiles: string[] = [];
    const duplicateFiles: string[] = [];
    const attachments: ChatAiAgentAttachment[] = [];

    for (const file of files) {
        const extension = genUtils.getFileExtension(file.name)?.toLowerCase();
        const fileName = file.name;

        if (!validExtensions.includes(extension)) {
            invalidFiles.push(file.name);
            continue;
        }

        // TODO check also docs attachments
        if (existingNames.has(fileName) || addedNames.has(fileName)) {
            duplicateFiles.push(file.name);
            continue;
        }

        addedNames.add(fileName);
        attachments.push({
            type: "localFile",
            name: file.name,
            contentType: file.type,
            file,
        });
    }

    return {
        attachments,
        invalidFiles,
        duplicateFiles,
    };
}

function reportValidationErrors({ invalidFiles = [], duplicateFiles = [] }: AttachmentValidationErrors) {
    if (!invalidFiles.length && !duplicateFiles.length) {
        return;
    }

    const errors = [
        invalidFiles.length ? `Unsupported file type: ${invalidFiles.join(", ")}` : null,
        duplicateFiles.length ? `Attachment already added: ${duplicateFiles.join(", ")}` : null,
    ].filter(Boolean);

    messagePublisher.reportError(errors.join(". "));
}

function getIcon(contentType: string): IconName {
    switch (contentType) {
        case "image/png":
        case "image/gif":
        case "image/jpeg":
        case "image/webp":
            return "filesystem";
        default:
            return "document2";
    }
}

export const chatAiAgentAttachmentsUtils = {
    validExtensions,
    validExtensionsAccept,
    prepareLocalFiles,
    reportValidationErrors,
    getIcon,
};
