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
        const blob = new Blob([item.value]);
        return blob.size;
    }, [item.value]);

    const canDiscard = item.type !== "Current View" && item.type !== "Current Database Name";
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
        if (canClick && item.state === "included" && isHovering) {
            return "cancel";
        }

        if (item.state === "excluded") {
            return "plus";
        }

        switch (item.type) {
            case "Current View":
                return "studio-configuration";
            case "Current Database Name":
                return "database";
            case "Current Index Definition":
                return "index";
            case "Current Document":
                return "document";
            case "Endpoints Response":
                return "endpoint";
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
                    { "opacity-50": item.state === "excluded" },
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
                <span className="text-truncate">{item.label}</span>
                {sizeInBytes > 1024 && <Icon icon="warning" color="warning" margin="ms-1" />}
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
    "Current View": "Current View",
    "Current Database Name": "Database Name",
    "Current Index Definition": "Index Definition",
    "Current Document": "Document",
    "Endpoints Response": "Endpoint Response",
};
