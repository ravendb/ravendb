import { useAppDispatch } from "components/store";
import { chatbotActions, ChatbotAttachedContext } from "../../store/chatbotSlice";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { useMemo } from "react";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";

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
    if (!attachedContexts.some((item) => item.isVisible)) {
        return null;
    }

    return (
        <div className={classNames("hstack flex-wrap", className)} style={{ gap: "4px" }}>
            {attachedContexts.map((item) => (
                <Item key={item.name} item={item} isReadOnly={isReadOnly} />
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

    const canClick = item.canDiscard && !isReadOnly;

    const { value: isHovering, setValue: setIsHovering } = useBoolean(false);

    const handleClick = () => {
        if (!canClick) {
            return;
        }

        if (item.isIncluded) {
            dispatch(chatbotActions.attachedContextDiscarded(item.name));
        } else {
            dispatch(chatbotActions.attachedContextIncluded(item.name));
        }
    };

    const iconColor = useMemo((): ThemeColor => {
        if (canClick && isHovering) {
            return "secondary";
        }

        if (item.isIncluded) {
            return "primary";
        } else {
            return "secondary";
        }
    }, [item.isIncluded, canClick, isHovering]);

    const iconName = useMemo((): IconName => {
        if (canClick && item.isIncluded && isHovering) {
            return "cancel";
        }

        if (item.isIncluded) {
            return item.iconName;
        } else {
            return "plus";
        }
    }, [item.isIncluded, canClick, isHovering]);

    if (!item.isVisible) {
        return null;
    }

    return (
        <div
            className={classNames(
                "hstack rounded-2 border border-secondary text-truncate",
                { "opacity-50": !item.isIncluded },
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
            <Icon icon={iconName} color={iconColor} />
            <span className="text-truncate">{item.label}</span>
        </div>
    );
}
