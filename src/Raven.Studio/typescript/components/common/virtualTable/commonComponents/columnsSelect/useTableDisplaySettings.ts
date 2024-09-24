import { Table as TanstackTable } from "@tanstack/react-table";
import genUtils from "common/generalUtils";
import useBoolean from "components/hooks/useBoolean";
import { useState } from "react";

export function useTableDisplaySettings<T>(table: TanstackTable<T>) {
    const { value: isDropdownOpen, setValue: setIsDropdownOpen } = useBoolean(false);

    const availableColumns = table.getAllColumns().filter((column) => column.getCanHide());
    const availableColumnsIds = availableColumns.map((x) => x.id);

    const [selectedColumnsIds, setSelectedColumnsIds] = useState(
        availableColumns.filter((column) => column.getIsVisible()).map((x) => x.id)
    );
    const [initialSelectedColumnsIds] = useState(selectedColumnsIds);

    const selectionState = genUtils.getSelectionState(availableColumnsIds, selectedColumnsIds);

    const handleToggleAll = () => {
        if (selectionState === "Empty") {
            setSelectedColumnsIds(availableColumnsIds);
        } else {
            setSelectedColumnsIds([]);
        }
    };

    const handleToggleOne = (id: string) => {
        if (selectedColumnsIds.includes(id)) {
            setSelectedColumnsIds(selectedColumnsIds.filter((x) => x !== id));
        } else {
            setSelectedColumnsIds([...selectedColumnsIds, id]);
        }
    };

    const handleOpenDropdown = () => {
        setSelectedColumnsIds(availableColumns.filter((column) => column.getIsVisible()).map((x) => x.id));
        setIsDropdownOpen(true);
    };

    const handleCloseDropdown = () => {
        setSelectedColumnsIds(initialSelectedColumnsIds);
        setIsDropdownOpen(false);
    };

    const handleToggleDropdown = () => {
        if (isDropdownOpen) {
            handleCloseDropdown();
        } else {
            handleOpenDropdown();
        }
    };

    const handleReset = () => {
        setSelectedColumnsIds(initialSelectedColumnsIds);
    };

    const handleSave = () => {
        availableColumns.forEach((column) => {
            column.toggleVisibility(getIsColumnSelected(column.id));
        });
        handleCloseDropdown();
    };

    const getIsColumnSelected = (id: string) => selectedColumnsIds.includes(id);

    return {
        isDropdownOpen,
        availableColumns,
        availableColumnsIds,
        selectedColumnsIds,
        selectionState,
        getIsColumnSelected,
        handlers: {
            handleToggleAll,
            handleToggleOne,
            handleSave,
            handleReset,
            handleToggleDropdown,
            handleCloseDropdown,
        },
    };
}
