import genUtils from "common/generalUtils";

interface UseCheckboxesOptions<T extends string | number> {
    allItems: T[];
    selectedItems: T[];
    setValue: (selectedItems: T[]) => void;
}

export function useCheckboxes<T extends string | number>({
    allItems: items,
    selectedItems,
    setValue,
}: UseCheckboxesOptions<T>) {
    const selectionState = genUtils.getSelectionState(items, selectedItems);

    const toggleOne = (item: T) => {
        if (selectedItems.includes(item)) {
            setValue(selectedItems.filter((x) => x !== item));
        } else {
            setValue([...selectedItems, item]);
        }
    };

    const toggleAll = () => {
        if (selectionState === "Empty") {
            setValue(items);
        } else {
            setValue([]);
        }
    };

    return {
        selectionState,
        toggleOne,
        toggleAll,
    };
}
