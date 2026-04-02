import genUtils from "common/generalUtils";
import messagePublisher from "common/messagePublisher";
import { ChatAiAgentAttachment } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";
import moment from "moment";
import IconName from "typings/server/icons";

const validExtensions = ["txt", "pdf", "jpg", "jpeg", "png", "gif", "webp"];
const validExtensionsAccept = validExtensions.map((extension) => `.${extension}`).join(",");
const clipboardFallbackNameByContentType: Partial<Record<string, string>> = {
    "application/pdf": "pasted-file.pdf",
    "image/gif": "pasted-image.gif",
    "image/jpeg": "pasted-image.jpg",
    "image/png": "pasted-image.png",
    "image/webp": "pasted-image.webp",
    "text/plain": "pasted-file.txt",
};

function getClipboardFallbackName(contentType: string) {
    const fallbackName = clipboardFallbackNameByContentType[contentType?.toLowerCase()] ?? "pasted-file";
    return appendLocalTimestampToFileName(fallbackName);
}

function normalizeLocalFile(file: File) {
    // Edge case: clipboard image payloads can surface as File objects without a usable name.
    // The File API allows File.name to be empty when the user agent cannot expose it, so we
    // generate a fallback before extension validation and deduplication: https://w3c.github.io/FileAPI/#file-attrs
    const normalizedName = file.name?.trim() || getClipboardFallbackName(file.type);

    if (normalizedName === file.name) {
        return file;
    }

    return new File([file], normalizedName, {
        type: file.type,
        lastModified: file.lastModified,
    });
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

function appendLocalTimestampToFileName(name: string, date = new Date(Date.now())) {
    const { baseName, extension } = splitFileName(name);
    const formattedLocalTimestamp = moment(date).format("YYYY-MM-DD_HH-mm-ss");

    return `${baseName}-${formattedLocalTimestamp}${extension}`;
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

function getLocalFilesFromClipboardData(clipboardData: DataTransfer) {
    const directFiles = Array.from(clipboardData.files || []);

    if (directFiles.length) {
        return directFiles;
    }

    return Array.from(clipboardData.items || [])
        .filter((item) => item.kind === "file")
        .map((item) => item.getAsFile())
        .filter((file): file is File => file != null);
}

function prepareConversationLocalFiles(
    files: File[],
    pendingAttachmentNames: string[],
    persistedAttachmentNames: string[] = []
) {
    return prepareLocalFiles(files, [...pendingAttachmentNames, ...persistedAttachmentNames]);
}

function prepareLocalFiles(
    files: File[],
    existingAttachmentNames: string[]
): {
    attachments: ChatAiAgentAttachment[];
    invalidFiles: string[];
} {
    const reservedNames = createReservedNames(existingAttachmentNames);

    const invalidFiles: string[] = [];
    const attachments: ChatAiAgentAttachment[] = [];

    for (const rawFile of files) {
        const file = normalizeLocalFile(rawFile);
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
    };
}

function reportValidationErrors(invalidFiles: string[]) {
    if (!invalidFiles.length) {
        return;
    }

    const error = `Unsupported file type: ${invalidFiles.join(", ")}`;
    messagePublisher.reportError(error);
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
    getLocalFilesFromClipboardData,
    prepareConversationLocalFiles,
    prepareLocalFiles,
    reportValidationErrors,
    getIcon,
};
