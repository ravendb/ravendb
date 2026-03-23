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

function splitFileName(name: string) {
    const lastDotIndex = name.lastIndexOf(".");

    if (lastDotIndex <= 0) {
        return {
            baseName: name,
            extension: "",
        };
    }

    return {
        baseName: name.slice(0, lastDotIndex),
        extension: name.slice(lastDotIndex),
    };
}

function createReservedNames(names: string[]) {
    return new Set(names.map((name) => name.toLowerCase()));
}

function createUniqueName(name: string, reservedNames: Set<string>) {
    const { baseName, extension } = splitFileName(name);
    const match = /^(.*)\((\d+)\)$/.exec(baseName);
    const normalizedBaseName = match ? match[1] : baseName;

    let candidate = name;
    let index = match ? Number(match[2]) : 0;

    while (reservedNames.has(candidate.toLowerCase())) {
        index += 1;
        candidate = `${normalizedBaseName}(${index})${extension}`;
    }

    reservedNames.add(candidate.toLowerCase());

    return candidate;
}

function prepareLocalFiles(
    files: File[],
    existingAttachmentNames: string[]
): {
    attachments: ChatAiAgentAttachment[];
    invalidFiles: string[];
    duplicateFiles: string[];
} {
    const reservedNames = createReservedNames(existingAttachmentNames);

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

        attachments.push({
            type: "localFile",
            name: createUniqueName(fileName, reservedNames),
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

    const errors = [invalidFiles.length ? `Unsupported file type: ${invalidFiles.join(", ")}` : null].filter(Boolean);

    if (errors.length) {
        messagePublisher.reportError(errors.join(". "));
    }
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
    createReservedNames,
    createUniqueName,
    prepareLocalFiles,
    reportValidationErrors,
    getIcon,
};
