import { useAppDispatch } from "components/store";
import { chatbotActions, ChatbotAttachedContext } from "../../store/chatbotSlice";
import { Icon } from "components/common/Icon";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";
import assertUnreachable from "components/utils/assertUnreachable";

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
            case "Endpoints Responses":
                return "endpoint";
            default:
                assertUnreachable(item.type);
        }
    };

    return (
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
        </div>
    );
}
