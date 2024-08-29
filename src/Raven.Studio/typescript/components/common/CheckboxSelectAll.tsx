import { Checkbox } from "components/common/Checkbox";
import { SelectionState } from "components/models/common";
import React from "react";
import { Badge } from "reactstrap";

interface CheckboxSelectAllProps {
    selectionState: SelectionState;
    toggleAll: () => void;
    allItemsCount?: number;
    selectedItemsCount?: number;
    size?: string;
    color?: string;
    title?: string;
}

export default function CheckboxSelectAll({
    selectionState,
    toggleAll,
    allItemsCount,
    selectedItemsCount,
    size = "md",
    color = "primary",
    title = "Select all or none",
}: CheckboxSelectAllProps) {
    return (
        <Checkbox
            size={size}
            toggleSelection={toggleAll}
            indeterminate={selectionState === "SomeSelected"}
            selected={selectionState === "AllSelected"}
            title={title}
            color={color}
        >
            <span className="text-uppercase small-label">
                {selectionState === "Empty" ? (
                    <span>Select all {allItemsCount ? <Badge color="secondary">{allItemsCount}</Badge> : null}</span>
                ) : (
                    <span>
                        Deselect all {selectedItemsCount ? <Badge color="secondary">{selectedItemsCount}</Badge> : null}
                    </span>
                )}
            </span>
        </Checkbox>
    );
}
