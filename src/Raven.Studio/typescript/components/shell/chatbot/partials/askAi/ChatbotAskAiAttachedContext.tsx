import "./ChatbotAskAiAttachedContext.scss";
import { useAppDispatch } from "components/store";
import { chatbotActions, ChatbotAttachedContext } from "../../store/chatbotSlice";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";
import assertUnreachable from "components/utils/assertUnreachable";
import { useMemo } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import genUtils from "common/generalUtils";
import ChatbotAskAiAttachedContextNewItem from "components/shell/chatbot/partials/askAi/ChatbotAskAiAttachedContextNewItem";
import Button from "react-bootstrap/Button";
import Code from "components/common/Code";

interface ChatbotAskAiAttachedContextProps {
    attachedContexts: ChatbotAttachedContext[];
    isReadOnly?: boolean;
    className?: string;
}

export default function ChatbotAskAiAttachedContext({
    attachedContexts,
    isReadOnly = false,
    className,
}: ChatbotAskAiAttachedContextProps) {
    if (attachedContexts.every((item) => !item.value)) {
        return null;
    }

    return (
        <div className={classNames("hstack flex-wrap", className)} style={{ gap: "4px" }}>
            {!isReadOnly && <ChatbotAskAiAttachedContextNewItem />}
            {attachedContexts.map((item) => (
                <Item key={item.id} item={item} isReadOnly={isReadOnly} />
            ))}
        </div>
    );
}

interface ContextItemProps {
    item: ChatbotAttachedContext;
    isReadOnly?: boolean;
}

function Item({ item, isReadOnly = false }: ContextItemProps) {
    const dispatch = useAppDispatch();

    const sizeInBytes = useMemo(() => {
        const stringValue = typeof item.value === "string" ? item.value : JSON.stringify(item.value);

        const blob = new Blob([stringValue]);
        return blob.size;
    }, [item.value]);

    const canRemove = item.type !== "View" && !isReadOnly;
    const canInclude = item.state === "excluded" && !isReadOnly;

    const hasSizeWaring = sizeInBytes > 1024; // 1KB size warning threshold
    const hasConfidentialData = item.type === "QueryResult";
    const hasAdditionalInfo = hasConfidentialData || hasSizeWaring;

    const { value: isHovering, setValue: setIsHovering } = useBoolean(false);

    const getIconColor = (): ThemeColor => {
        if (item.state === "included") {
            return "primary";
        } else {
            return "secondary";
        }
    };

    const getIconName = (): IconName => {
        if (canInclude && isHovering) {
            return "plus";
        }

        switch (item.type) {
            case "View":
                return "studio-configuration";
            case "DatabaseName":
                return "database";
            case "IndexName":
                return "index";
            case "CollectionName":
                return "documents";
            case "DocumentId":
                return "document";
            case "QueryResult":
                return "query";
            case "QueryError":
                return "danger";
            default:
                assertUnreachable(item.type);
        }
    };

    return (
        <div
            className={classNames(
                "attached-context hstack rounded-1 border border-color-light lh-base text-truncate bg-faded-primary",
                {
                    "cursor-pointer hover-filter opacity-50": canInclude,
                }
            )}
            onMouseEnter={() => setIsHovering(true)}
            onMouseLeave={() => setIsHovering(false)}
            onClick={() => dispatch(chatbotActions.attachedContextIncluded(item.id))}
        >
            <PopoverWithHoverWrapper message={<LabelTooltipContent item={item} />} wrapperClassName="mw-100">
                <div
                    className={classNames("context-label rounded-1 hstack", {
                        "border-end border-color-light": hasAdditionalInfo,
                    })}
                >
                    <Icon icon={getIconName()} color={getIconColor()} />
                    <span className="text-truncate text-body">{item.label}</span>
                    {canRemove && (
                        <Button
                            variant="link"
                            className="text-muted p-0 hover-filter"
                            onClick={() => dispatch(chatbotActions.attachedContextRemoved(item.id))}
                            size="xs"
                        >
                            <Icon icon="cancel" margin="ms-1" size="xs" title="Remove context" />
                        </Button>
                    )}
                </div>
            </PopoverWithHoverWrapper>
            {hasAdditionalInfo && (
                <div className="context-additional-info bg-faded-primary hstack text-body rounded-1 gap-1">
                    {hasConfidentialData && (
                        <PopoverWithHoverWrapper message="This content may contain confidential data.">
                            <Icon icon="shield" color="primary" margin="m-0" size="xs" />
                        </PopoverWithHoverWrapper>
                    )}
                    {hasSizeWaring && <span>{genUtils.formatBytesToSize(sizeInBytes)}</span>}
                </div>
            )}
        </div>
    );
}

function LabelTooltipContent({ item }: { item: ChatbotAttachedContext }) {
    return (
        <>
            <div className="fw-bold">{tooltipTitles[item.type]}</div>
            {item.type === "QueryResult" ? (
                <Code code={item.query} language="rql" isActionsHidden={true} className="mt-1" />
            ) : (
                <div className="word-break">{item.label}</div>
            )}
        </>
    );
}

const tooltipTitles: Record<ChatbotAttachedContext["type"], string> = {
    View: "Current view",
    DatabaseName: "Database name",
    DocumentId: "Document ID",
    IndexName: "Index name",
    CollectionName: "Collection name",
    QueryResult: "Query result",
    QueryError: "Query error",
};
