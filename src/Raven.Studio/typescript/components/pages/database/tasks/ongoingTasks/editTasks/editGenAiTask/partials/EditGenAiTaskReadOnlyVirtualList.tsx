import { useVirtualizer, VirtualItem } from "@tanstack/react-virtual";
import AceEditor from "components/common/ace/AceEditor";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef } from "react";
import Badge from "react-bootstrap/Badge";
import classNames from "classnames";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import ReactAce from "react-ace";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { FieldPath, useFormContext } from "react-hook-form";
import EditGenAiTaskAttachmentsButton from "./EditGenAiTaskAttachmentsButton";
import EditGenAiTaskReasoning from "./EditGenAiTaskReasoning";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";

interface EditGenAiTaskReadOnlyVirtualListProps {
    data: {
        value: string;
        attachments?: Raven.Server.Documents.ETL.Providers.AI.AiAttachment[];
        conversationDocument?: Raven.Server.Documents.Handlers.AI.Agents.ConversationDocument;
        isCached?: boolean;
    }[];
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
}

export default function EditGenAiTaskReadOnlyVirtualList({ data, name }: EditGenAiTaskReadOnlyVirtualListProps) {
    const listRef = useRef<HTMLDivElement>(null);

    const { setValue } = useFormContext<EditGenAiTaskFormData>();

    const virtualizer = useVirtualizer({
        count: data?.length ?? 0,
        estimateSize: () => 200,
        getScrollElement: () => listRef.current,
        overscan: 5,
    });

    if (!data || data.length === 0) {
        return null;
    }

    return (
        <div className="flex-grow-1 overflow-auto" ref={listRef}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const entry = data[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            className="py-1"
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                transition: "unset",
                            }}
                        >
                            {name === "playgroundModelOutputs" && !entry.value && (
                                <RichAlert variant="info" className="mb-1">
                                    Model output is cached. You can{" "}
                                    <Button
                                        variant="link"
                                        className="text-decoration-underline text-emphasis p-0"
                                        size="sm"
                                        onClick={() => setValue("isForceSendingCachedObjects", true)}
                                    >
                                        enable &quot;Force reprocess&quot;
                                    </Button>{" "}
                                    and test again to see the results.
                                </RichAlert>
                            )}
                            {entry.conversationDocument && (
                                <EditGenAiTaskReasoning conversationDocument={entry.conversationDocument} />
                            )}
                            <EditorWrapper
                                name={name}
                                rowIndex={virtualRow.index}
                                rowKey={virtualRow.key}
                                value={entry.value}
                                attachments={entry.attachments}
                            />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

interface EditorWrapperProps {
    rowIndex: number;
    rowKey: VirtualItem["key"];
    name: EditGenAiTaskReadOnlyVirtualListProps["name"];
    value: string;
    attachments?: Raven.Server.Documents.ETL.Providers.AI.AiAttachment[];
}

function EditorWrapper({ rowIndex, name, rowKey, value, attachments }: EditorWrapperProps) {
    const dispatch = useAppDispatch();

    const hoverIndex = useAppSelector(editGenAiTaskSelectors.hoverIndex);

    const getTooltipText = (): string => {
        if (name === "playgroundContexts") {
            return "Context object ID";
        }
        if (name === "playgroundModelOutputs") {
            return "Model output object ID";
        }

        return null;
    };

    return (
        <div
            style={{ position: "relative" }}
            onMouseEnter={() => dispatch(editGenAiTaskActions.hoverIndexSet(rowIndex))}
            onMouseLeave={() => dispatch(editGenAiTaskActions.hoverIndexSet(null))}
            className={classNames({
                "ace-hover": hoverIndex === rowIndex,
            })}
        >
            <Editor key={rowKey} value={value} />
            <div style={{ position: "absolute", bottom: 10, right: 40 }} className="d-flex gap-1">
                <EditGenAiTaskAttachmentsButton attachments={attachments} />
                <Badge bg="secondary" title={getTooltipText()} className="d-flex align-items-center">
                    {rowIndex + 1}
                </Badge>
            </div>
        </div>
    );
}

interface EditorProps {
    value: string;
}

function Editor({ value }: EditorProps) {
    const aceRef = useRef<ReactAce>(null);

    return (
        <AceEditor
            aceRef={aceRef}
            mode="json"
            value={value}
            readOnly={true}
            actions={[{ component: <AceEditor.FullScreenAction /> }]}
            isFullScreenLabelHidden
        />
    );
}
