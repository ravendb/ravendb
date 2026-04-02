import "./ChatAiAgentAttachmentsDropzone.scss";
import { ChatAiAgentFormData } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentValidation";
import useBoolean from "components/hooks/useBoolean";
import { useEffect, useRef, DragEvent } from "react";
import { UseFieldArrayReturn } from "react-hook-form";
import { chatAiAgentAttachmentsUtils } from "components/pages/database/aiHub/aiAgents/chat/utils/chatAiAgentAttachmentsUtils";
import { useAppSelector } from "components/store";
import { chatAiAgentSelectors } from "components/pages/database/aiHub/aiAgents/chat/store/chatAiAgentSlice";

const fileIcons = require("Content/img/dropzone-file-icons.png");

interface ChatAiAgentAttachmentsDropzoneProps {
    attachmentsFieldsArray: UseFieldArrayReturn<ChatAiAgentFormData, "attachments", "id">;
}

export default function ChatAiAgentAttachmentsDropzone({
    attachmentsFieldsArray,
}: ChatAiAgentAttachmentsDropzoneProps) {
    // It prevents overlay flickering because of dragleave/dragleave events firing when dragging files over child elements of the overlay
    const dragCounterRef = useRef(0);
    const conversationDocument = useAppSelector(chatAiAgentSelectors.document);

    const { value: isDraggingFiles, setTrue: showOverlay, setFalse: hideOverlay } = useBoolean(false);

    const appendFiles = (files: File[]) => {
        const { attachments, invalidFiles } = chatAiAgentAttachmentsUtils.prepareConversationLocalFiles(
            files,
            attachmentsFieldsArray.fields.map((x) => x.name),
            conversationDocument.data?.["@metadata"]?.["@attachments"]?.map((a) => a.Name) ?? []
        );

        if (attachments.length) {
            attachmentsFieldsArray.append(attachments);
        }

        chatAiAgentAttachmentsUtils.reportValidationErrors(invalidFiles);
    };

    const onDrop = (files: File[]) => {
        if (!files.length) {
            return;
        }

        hideOverlay();
        dragCounterRef.current = 0;

        appendFiles(files);
    };

    useEffect(() => {
        const handleDragEnter = (event: globalThis.DragEvent) => {
            if (!isFileDragEvent(event)) {
                return;
            }

            event.preventDefault();
            dragCounterRef.current += 1;
            showOverlay();
        };

        const handleDragOver = (event: globalThis.DragEvent) => {
            if (!isFileDragEvent(event)) {
                return;
            }

            event.preventDefault();
            showOverlay();
        };

        const handleDragLeave = (event: globalThis.DragEvent) => {
            if (!isFileDragEvent(event)) {
                return;
            }

            event.preventDefault();
            dragCounterRef.current = Math.max(dragCounterRef.current - 1, 0);

            if (dragCounterRef.current === 0) {
                hideOverlay();
            }
        };

        const handleWindowDrop = (event: globalThis.DragEvent) => {
            if (!isFileDragEvent(event)) {
                return;
            }

            event.preventDefault();
            onDrop(Array.from(event.dataTransfer?.files || []));
        };

        window.addEventListener("dragenter", handleDragEnter);
        window.addEventListener("dragover", handleDragOver);
        window.addEventListener("dragleave", handleDragLeave);
        window.addEventListener("drop", handleWindowDrop);

        return () => {
            window.removeEventListener("dragenter", handleDragEnter);
            window.removeEventListener("dragover", handleDragOver);
            window.removeEventListener("dragleave", handleDragLeave);
            window.removeEventListener("drop", handleWindowDrop);
        };
    }, [attachmentsFieldsArray.fields, hideOverlay, showOverlay]);

    const handleOverlayDragOver = (event: DragEvent<HTMLDivElement>) => {
        event.preventDefault();
    };

    const handleOverlayDragLeave = (event: DragEvent<HTMLDivElement>) => {
        event.preventDefault();

        const nextTarget = event.relatedTarget;

        if (nextTarget instanceof Node && event.currentTarget.contains(nextTarget)) {
            return;
        }

        if (event.currentTarget === event.target) {
            dragCounterRef.current = 0;
            hideOverlay();
        }
    };

    const handleOverlayDrop = (event: DragEvent<HTMLDivElement>) => {
        event.preventDefault();
        event.stopPropagation();
        onDrop(Array.from(event.dataTransfer.files || []));
    };

    return (
        <div className="chat-ai-agent-attachments-dropzone">
            {isDraggingFiles && (
                <div
                    className="dropzone-overlay"
                    onDragOver={handleOverlayDragOver}
                    onDragLeave={handleOverlayDragLeave}
                    onDrop={handleOverlayDrop}
                >
                    <div className="dropzone-content text-center">
                        <img src={fileIcons} alt="File icons" className="mb-4" />
                        <div className="fs-3 fw-bold">Drop files to add them to the conversation</div>
                        <div className="text-muted small">
                            Supported types:{" "}
                            {chatAiAgentAttachmentsUtils.validExtensions.map((extension) => `.${extension}`).join(", ")}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

function isFileDragEvent(event: globalThis.DragEvent) {
    return Array.from(event.dataTransfer?.types || []).includes("Files");
}
