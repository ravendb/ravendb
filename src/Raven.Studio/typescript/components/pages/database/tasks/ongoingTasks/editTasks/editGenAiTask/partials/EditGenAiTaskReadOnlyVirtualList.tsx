import { useVirtualizer } from "@tanstack/react-virtual";
import AceEditor from "components/common/ace/AceEditor";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef } from "react";
import Badge from "react-bootstrap/Badge";
import classNames from "classnames";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import ReactAce from "react-ace";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import { FieldPath } from "react-hook-form";

interface EditGenAiTaskReadOnlyVirtualListProps {
    data: string[];
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
}

export default function EditGenAiTaskReadOnlyVirtualList({ data, name }: EditGenAiTaskReadOnlyVirtualListProps) {
    const dispatch = useAppDispatch();

    const hoverIndex = useAppSelector(editGenAiTaskSelectors.hoverIndex);

    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: data?.length ?? 0,
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
                            className={classNames("py-1", {
                                "ace-hover": hoverIndex === virtualRow.index,
                            })}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                transition: "unset",
                            }}
                            onMouseEnter={() => dispatch(editGenAiTaskActions.hoverIndexSet(virtualRow.index))}
                            onMouseLeave={() => dispatch(editGenAiTaskActions.hoverIndexSet(null))}
                        >
                            <div style={{ position: "relative" }}>
                                <Editor key={virtualRow.key} value={entry} />
                                <Badge
                                    bg="secondary"
                                    style={{ position: "absolute", bottom: 10, right: 40 }}
                                    title={getTooltipText()}
                                >
                                    {virtualRow.index + 1}
                                </Badge>
                            </div>
                        </div>
                    );
                })}
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
        />
    );
}
