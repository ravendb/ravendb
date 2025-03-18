import { PopoverWithHover } from "components/common/PopoverWithHover";
import ToggleLimitBadge from "components/common/toggles/partials/ToggleLimitBadge";
import { InputItem } from "components/models/common";
import { useState } from "react";
import Popover from "react-bootstrap/Popover";
import genUtils from "common/generalUtils";

interface ToggleItemLabelProps<T extends string | number = string> {
    id: string;
    inputItem: InputItem<T>;
}

export default function ToggleItemLabel<T extends string | number = string>({
    id,
    inputItem,
}: ToggleItemLabelProps<T>) {
    const [target, setTarget] = useState<HTMLElement>();

    return (
        <>
            <label htmlFor={id} ref={setTarget}>
                <span>{inputItem.label}</span>
                {inputItem.count !== null && inputItem.limit ? (
                    <ToggleLimitBadge count={inputItem.count} limit={inputItem.limit} />
                ) : inputItem.count != null ? (
                    <span className="multi-toggle-item-count">
                        {genUtils.formatNumberToStringFixed(inputItem.count, 0)}
                    </span>
                ) : null}
            </label>
            {inputItem.popover && (
                <PopoverWithHover target={target} placement={inputItem.popoverPlacement ?? "top"}>
                    <Popover.Body>{inputItem.popover}</Popover.Body>
                </PopoverWithHover>
            )}
        </>
    );
}
