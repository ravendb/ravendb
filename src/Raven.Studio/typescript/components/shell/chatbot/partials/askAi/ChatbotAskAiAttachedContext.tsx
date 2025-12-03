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
            <ChatbotAskAiAttachedContextNewItem />
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

    const canDiscard = item.type !== "View";
    const canClick = canDiscard && !isReadOnly;

    const { value: isHovering, setValue: setIsHovering } = useBoolean(false);

    if (!item.value) {
        return null;
    }

    const handleClick = () => {
        if (!canClick) {
            return;
        }

        if (item.state === "included") {
            dispatch(chatbotActions.attachedContextExcluded(item.id));
        } else {
            dispatch(chatbotActions.attachedContextIncluded(item.id));
        }
    };

    const getIconColor = (): ThemeColor => {
        if (canClick && isHovering) {
            return "secondary";
        }

        if (item.state === "included") {
            return "primary";
        } else {
            return "secondary";
        }
    };

    const getIconName = (): IconName => {
        switch (item.type) {
            case "View":
                return "studio-configuration";
            case "DatabaseName":
                return "database";
            case "IndexName":
                return "index";
            case "CollectionName":
                return "document2";
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
        <PopoverWithHoverWrapper
            message={<TooltipContent sizeInBytes={sizeInBytes} type={item.type} />}
            wrapperClassName="mw-100"
        >
            <div
                className={classNames(
                    "hstack rounded-2 border border-secondary text-truncate",
                    { "opacity-50 ": item.state === "excluded" },
                    { "cursor-pointer hover-filter": canClick }
                )}
                onMouseEnter={() => setIsHovering(true)}
                onMouseLeave={() => setIsHovering(false)}
                style={{
                    padding: "1px 6px",
                    fontSize: "12px",
                }}
                onClick={handleClick}
            >
                <Icon icon={getIconName()} color={getIconColor()} />
                <span
                    className={classNames("text-truncate", {
                        "text-decoration-line-through ": item.state === "excluded",
                    })}
                >
                    {item.label}
                </span>
                {sizeInBytes > 1024 && <Icon icon="warning" color="warning" margin="ms-1" />}
                {canClick && (
                    <Button
                        variant="link"
                        className="text-muted p-0"
                        onClick={() => dispatch(chatbotActions.attachedContextRemoved(item.id))}
                        size="xs"
                    >
                        <Icon icon="cancel" margin="ms-1" size="xs" />
                    </Button>
                )}
            </div>
        </PopoverWithHoverWrapper>
    );
}

interface TooltipContentProps {
    type: ChatbotAttachedContext["type"];
    sizeInBytes: number;
}

function TooltipContent({ type, sizeInBytes }: TooltipContentProps) {
    return (
        <>
            <div className="small-label mb-1">{tooltipTitles[type]}</div>
            <div>Size: {genUtils.formatBytesToSize(sizeInBytes)}</div>
        </>
    );
}

const tooltipTitles: Record<ChatbotAttachedContext["type"], string> = {
    View: "Current View",
    DatabaseName: "Database Name",
    DocumentId: "Document ID",
    IndexName: "Index Name",
    CollectionName: "Collection Name",
    QueryResult: "Query Result",
    QueryError: "Query Error",
};
