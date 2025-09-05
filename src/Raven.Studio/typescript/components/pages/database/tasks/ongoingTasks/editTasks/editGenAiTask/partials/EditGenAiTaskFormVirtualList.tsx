import { useVirtualizer } from "@tanstack/react-virtual";
import { EmptySet } from "components/common/EmptySet";
import { FormAceEditor } from "components/common/Form";
import { useRef } from "react";
import { FieldArrayWithId, FieldPath, useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import Badge from "react-bootstrap/Badge";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import EditGenAiTaskAttachmentsButton from "./EditGenAiTaskAttachmentsButton";

interface EditGenAiTaskFormVirtualListProps {
    fields: FieldArrayWithId<EditGenAiTaskFormData, "playgroundContexts" | "playgroundModelOutputs">[];
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
    isReadOnly: boolean;
    handleRemove: (index: number) => void;
}

export default function EditGenAiTaskFormVirtualList({
    fields,
    name,
    isReadOnly,
    handleRemove,
}: EditGenAiTaskFormVirtualListProps) {
    const dispatch = useAppDispatch();

    const hoverIndex = useAppSelector(editGenAiTaskSelectors.hoverIndex);

    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: fields.length,
        estimateSize: () => 200,
        getScrollElement: () => listRef.current,
        overscan: 5,
    });

    const getTooltipText = (): string => {
        if (name === "playgroundContexts") {
            return "Context object ID";
        }
        if (name === "playgroundModelOutputs") {
            return "Model output object ID";
        }

        return null;
    };

    if (fields.length === 0) {
        return <EmptySet>Empty list</EmptySet>;
    }

    return (
        <div className="flex-grow-1 overflow-auto" ref={listRef}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const field = fields[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            className={classNames("py-1", {
                                "ace-hover": hoverIndex === field.idx,
                            })}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                transition: "unset",
                            }}
                            onMouseEnter={() => dispatch(editGenAiTaskActions.hoverIndexSet(field.idx))}
                            onMouseLeave={() => dispatch(editGenAiTaskActions.hoverIndexSet(null))}
                        >
                            <div style={{ position: "relative" }}>
                                <Editor
                                    key={field.id}
                                    index={virtualRow.index}
                                    name={name}
                                    isReadOnly={isReadOnly}
                                    handleRemove={handleRemove}
                                />
                                <div style={{ position: "absolute", bottom: 10, right: 40 }} className="d-flex gap-1">
                                    <EditGenAiTaskAttachmentsButton attachments={field.attachments} />
                                    <Badge
                                        bg="secondary"
                                        title={getTooltipText()}
                                        className="d-flex align-items-center"
                                    >
                                        {field.idx != null ? field.idx + 1 : "?"}
                                    </Badge>
                                </div>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

interface EditorProps {
    index: number;
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
    isReadOnly: boolean;
    handleRemove: (index: number) => void;
}

function Editor({ index, name, isReadOnly, handleRemove }: EditorProps) {
    const aceRef = useRef<ReactAce>(null);

    const { control } = useFormContext<EditGenAiTaskFormData>();

    return (
        <FormAceEditor
            aceRef={aceRef}
            control={control}
            name={`${name}.${index}.value`}
            mode="json"
            readOnly={isReadOnly}
            actions={[
                { component: <AceEditor.FullScreenAction /> },
                { component: <AceEditor.FormatAction /> },
                !isReadOnly
                    ? {
                          component: <AceEditor.DeleteAction onDelete={() => handleRemove(index)} />,
                      }
                    : null,
            ]}
            isFullScreenLabelHidden
        />
    );
}
