import "./RadioButtons.scss";
import { ReactNode } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useUniqueId from "components/hooks/useUniqueId";

export interface RadioButtonsItem<T extends string> {
    value: T;
    label: string;
    helpMessage?: ReactNode;
}

interface RadioButtonsProps<T extends string> {
    items: RadioButtonsItem<T>[];
    selected: T;
    onSelect: (value: T) => void;
    "aria-label": string;
    isDisabled?: boolean;
    className?: string;
}

export default function RadioButtons<T extends string>({
    items,
    selected,
    onSelect,
    "aria-label": ariaLabel,
    isDisabled,
    className,
}: RadioButtonsProps<T>) {
    const groupName = useUniqueId("radio-buttons");

    return (
        <div className={classNames("radio-buttons", className)} role="radiogroup" aria-label={ariaLabel}>
            {items.map((item) => (
                <label key={item.value} className={classNames("radio-buttons-item", { disabled: isDisabled })}>
                    <input
                        type="radio"
                        name={groupName}
                        value={item.value}
                        checked={item.value === selected}
                        onChange={() => onSelect(item.value)}
                        disabled={isDisabled}
                    />
                    <span>{item.label}</span>
                    {item.helpMessage && (
                        <PopoverWithHoverWrapper message={item.helpMessage}>
                            <Icon icon="help" margin="m-0" />
                        </PopoverWithHoverWrapper>
                    )}
                </label>
            ))}
        </div>
    );
}
