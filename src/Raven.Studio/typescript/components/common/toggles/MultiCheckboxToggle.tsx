import "./Toggles.scss";
import useUniqueId from "components/hooks/useUniqueId";
import classNames from "classnames";
import useBoolean from "components/hooks/useBoolean";
import { InputItem } from "components/models/common";
import { Icon } from "../Icon";
import Button from "react-bootstrap/Button";
import ToggleItemLabel from "components/common/toggles/partials/ToggleItemLabel";

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
    const uniqueId = useUniqueId("multi-checkbox-toggle");

    const {
        value: isSelectedAll,
        toggle: toggleIsSelectAll,
        setFalse: setIsSelectAllFalse,
        setTrue: setIsSelectAllTrue,
    } = useBoolean(!!selectAll && selectedItems.length === 0);

    const toggleItem = (toggleValue: boolean, inputItemValue: T) => {
        if (toggleValue) {
            if (isSelectedAll) {
                setSelectedItems([inputItemValue]);
                setIsSelectAllFalse();
            } else {
                setSelectedItems([...selectedItems, inputItemValue]);
            }
        } else {
            const filteredSelectedItems = selectedItems.filter((x) => x !== inputItemValue);

            if (selectAll && filteredSelectedItems.length === 0) {
                setIsSelectAllTrue();
            }
            setSelectedItems(filteredSelectedItems);
        }
    };

    const onChangeSelectAll = () => {
        toggleIsSelectAll();
        setSelectedItems(inputItems.map((x) => x.value));
    };

    return (
        <div className={classNames("multi-toggle", className)}>
            {label && <div className="small-label ms-1 mb-1">{label}</div>}
            <div className="multi-toggle-list">
                {selectAll && (
                    <>
                        <Button
                            variant="secondary"
                            className={classNames("multi-toggle-button", { "clear-selected": !isSelectedAll })}
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
                    <Item
                        key={uniqueId + inputItem.value}
                        id={uniqueId + inputItem.value}
                        inputItem={inputItem}
                        selectAllEnabled={isSelectedAll}
                        selectedItems={selectedItems}
                        toggleItem={toggleItem}
                    />
                ))}
            </div>
        </div>
    );
}

interface ItemProps<T extends string | number = string> {
    id: string;
    inputItem: InputItem<T>;
    selectAllEnabled?: boolean;
    selectedItems: T[];
    toggleItem: (toggleValue: boolean, inputItemValue: T) => void;
}

function Item<T extends string | number = string>({
    id,
    inputItem,
    selectAllEnabled,
    selectedItems,
    toggleItem,
}: ItemProps<T>) {
    return (
        <div className="flex-horizontal">
            {inputItem.verticalSeparatorLine && <div className="vr" />}
            <div className="multi-toggle-item">
                <input
                    id={id}
                    type="checkbox"
                    name={id}
                    checked={!selectAllEnabled && selectedItems.includes(inputItem.value)}
                    onChange={(x) => toggleItem(x.currentTarget.checked, inputItem.value)}
                />
                <ToggleItemLabel id={id} inputItem={inputItem} />
            </div>
        </div>
    );
}
