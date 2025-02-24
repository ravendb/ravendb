import { ConditionalPopover } from "components/common/ConditionalPopover";
import ToggleLimitBadge from "components/common/toggles/partials/ToggleLimitBadge";
import { InputItem } from "components/models/common";

interface ToggleItemLabelProps<T extends string | number = string> {
    id: string;
    inputItem: InputItem<T>;
}

export default function ToggleItemLabel<T extends string | number = string>({
    id,
    inputItem,
}: ToggleItemLabelProps<T>) {
    return (
        <ConditionalPopover
            popoverPlacement={inputItem.popoverPlacement ?? "top"}
            conditions={{
                isActive: inputItem.popover != null,
                message: inputItem.popover,
            }}
        >
            <label htmlFor={id}>
                <span>{inputItem.label}</span>
                {inputItem.count !== null && inputItem.limit ? (
                    <ToggleLimitBadge count={inputItem.count} limit={inputItem.limit} />
                ) : (
                    <span className="multi-toggle-item-count">{inputItem.count}</span>
                )}
            </label>
        </ConditionalPopover>
    );
}
