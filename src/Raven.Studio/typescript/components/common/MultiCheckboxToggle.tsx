import React from "react";
import useId from "hooks/useId";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { InputItem } from "components/models/common";
import "./Toggles.scss";
import { Icon } from "./Icon";
import { Button } from "reactstrap";

interface MultiCheckboxToggleProps<T extends string | number = string> {
    inputItems: InputItem<T>[];
    selectedItems: T[];
    setSelectedItems: (x: T[]) => void;
    selectAll?: boolean;
    selectAllCount?: number;
    selectAllLabel?: string;
    className?: string;
    label?: string;
}

export function MultiCheckboxToggle<T extends string | number = string>({
    inputItems,
    selectedItems,
    setSelectedItems,
    selectAll,
    selectAllLabel,
    selectAllCount,
    className,
    label,
}: MultiCheckboxToggleProps<T>) {
    const uniqueId = useId("multi-checkbox-toggle");

    const {
        value: selectAllEnabled,
        toggle: toggleSelectAllEnabled,
        setFalse: setSelectAllEnabledFalse,
        setTrue: setSelectAllEnabledTrue,
    } = useBoolean(!!selectAll && selectedItems.length === 0);

    const toggleItem = (toggleValue: boolean, inputItemValue: T) => {
        if (toggleValue) {
            if (selectAllEnabled) {
                setSelectedItems([inputItemValue]);
                setSelectAllEnabledFalse();
            } else {
                setSelectedItems([...selectedItems, inputItemValue]);
            }
        } else {
            const filteredSelectedItems = selectedItems.filter((x) => x !== inputItemValue);

            if (selectAll && filteredSelectedItems.length === 0) {
                setSelectAllEnabledTrue();
            }
            setSelectedItems(filteredSelectedItems);
        }
    };

    const onChangeSelectAll = () => {
        toggleSelectAllEnabled();
        setSelectedItems(inputItems.map((x) => x.value));
    };

    return (
        <div className={classNames("multi-toggle", className)}>
            {label && <div className="small-label ms-1 mb-1">{label}</div>}
            <div className="multi-toggle-list">
                {selectAll && (
                    <>
                        <Button
                            className={classNames("multi-toggle-button", { "clear-selected": !selectAllEnabled })}
                            size="sm"
                            onClick={onChangeSelectAll}
                            title="Toggle all"
                        >
                            <div className="label-span">
                                <div className="label-select-all">
                                    {selectAllLabel ?? <Icon icon="accept" margin="m-0" />}
                                </div>
                                <Icon icon="clear" className="label-clear" margin="m-0" />
                            </div>

                            {selectAllCount != null && (
                                <span className="multi-toggle-item-count ms-1">{selectAllCount}</span>
                            )}
                        </Button>
                        <div className="vr" />
                    </>
                )}
                {inputItems.map((inputItem) => (
                    <div key={uniqueId + inputItem.value} className="flex-horizontal">
                        {inputItem.verticalSeparatorLine && <div className="vr" />}
                        <div className="multi-toggle-item">
                            <input
                                id={uniqueId + inputItem.value}
                                type="checkbox"
                                name={uniqueId + inputItem.value}
                                checked={!selectAllEnabled && selectedItems.includes(inputItem.value)}
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
                    </div>
                ))}
            </div>
        </div>
    );
}
