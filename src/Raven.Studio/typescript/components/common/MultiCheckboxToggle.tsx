import React from "react";
import useId from "hooks/useId";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { InputItem } from "components/models/common";
import "./Toggles.scss";

interface MultiCheckboxToggleProps<T extends string | number = string> {
    inputItems: InputItem<T>[];
    selectedItems: T[];
    setSelectedItems: (x: T[]) => void;
    itemSelectAll?: InputItem;
    className?: string;
    label?: string;
}

export function MultiCheckboxToggle<T extends string | number = string>({
    inputItems,
    selectedItems,
    setSelectedItems,
    itemSelectAll,
    className,
    label,
}: MultiCheckboxToggleProps<T>) {
    const uniqueId = useId("multi-checkbox-toggle");

    const {
        value: isSelectedAll,
        toggle: toggleIsSelectedAll,
        setFalse: setIsSelectedAllFalse,
        setTrue: setIsSelectedAllTrue,
    } = useBoolean(!!itemSelectAll && selectedItems.length === 0);

    const toggleItem = (toggleValue: boolean, inputItemValue: T) => {
        if (toggleValue) {
            if (isSelectedAll) {
                setSelectedItems([inputItemValue]);
                setIsSelectedAllFalse();
            } else {
                setSelectedItems([...selectedItems, inputItemValue]);
            }
        } else {
            const filteredSelectedItems = selectedItems.filter((x) => x !== inputItemValue);

            if (itemSelectAll && filteredSelectedItems.length === 0) {
                setIsSelectedAllTrue();
            }
            setSelectedItems(filteredSelectedItems);
        }
    };

    const onChangeSelectAll = () => {
        toggleIsSelectedAll();
        setSelectedItems(inputItems.map((x) => x.value));
    };

    return (
        <div className={classNames("multi-toggle", className)}>
            {label && <div className="small-label ms-1 mb-1">{label}</div>}
            <div className="multi-toggle-list">
                {itemSelectAll && (
                    <div className="multi-toggle-item">
                        <input
                            id={uniqueId + itemSelectAll.value}
                            type="checkbox"
                            name={uniqueId + itemSelectAll.value}
                            checked={isSelectedAll}
                            onChange={onChangeSelectAll}
                        />
                        <label htmlFor={uniqueId + itemSelectAll.value}>
                            <span>
                                {itemSelectAll.label}
                                {itemSelectAll.count >= 0 && (
                                    <span className="multi-toggle-item-count">{itemSelectAll.count}</span>
                                )}
                            </span>
                        </label>
                    </div>
                )}
                {inputItems.map((inputItem) => (
                    <div className="multi-toggle-item" key={uniqueId + inputItem.value}>
                        {inputItem.verticalSeparatorLine && <div className="vr"></div>}
                        <input
                            id={uniqueId + inputItem.value}
                            type="checkbox"
                            name={uniqueId + inputItem.value}
                            checked={!isSelectedAll && selectedItems.includes(inputItem.value)}
                            onChange={(x) => toggleItem(x.currentTarget.checked, inputItem.value)}
                        />
                        <label htmlFor={uniqueId + inputItem.value}>
                            <span>
                                {inputItem.label}
                                {inputItem.count >= 0 && (
                                    <span className="multi-toggle-item-count">{inputItem.count}</span>
                                )}
                            </span>
                        </label>
                    </div>
                ))}
            </div>
        </div>
    );
}
