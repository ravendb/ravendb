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
        <PopoverWithHoverWrapper
            message={<TooltipContent sizeInBytes={sizeInBytes} type={item.type} />}
            wrapperClassName="mw-100"
        >
            <div
                className={classNames(
                    "attached-context hstack rounded-1 border border-color-light lh-base text-truncate",
                    {
                        "cursor-pointer hover-filter opacity-50": canInclude,
                    }
                )}
                onMouseEnter={() => setIsHovering(true)}
                onMouseLeave={() => setIsHovering(false)}
                style={{
                    padding: "1px 4.3px",
                    fontSize: "0.875em",
                    height: "22px",
                    maxWidth: "128px",
                }}
                onClick={() => dispatch(chatbotActions.attachedContextIncluded(item.id))}
            >
                <Icon icon={getIconName()} color={getIconColor()} />
                <span className="text-truncate text-body" title={item.label}>
                    {item.label}
                </span>
                {sizeInBytes > 1024 && <Icon icon="warning" color="warning" margin="ms-1" />}
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
    );
}

interface TooltipContentProps {
    type: ChatbotAttachedContext["type"];
    sizeInBytes: number;
}

function TooltipContent({ type, sizeInBytes }: TooltipContentProps) {
    return (
        <>
            <div className="fw-bold">{tooltipTitles[type]}</div>
            <div>Size: {genUtils.formatBytesToSize(sizeInBytes)}</div>
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
