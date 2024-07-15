import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import { useEffect } from "react";

interface UseStudioSearchKeyboardEventsProps {
    refs: {
        inputRef: React.RefObject<HTMLInputElement>;
        dropdownRef: React.RefObject<any>;
        serverColumnRef: React.RefObject<HTMLDivElement>;
        databaseColumnRef: React.RefObject<HTMLDivElement>;
    };
    results: StudioSearchResult;
    activeItem: StudioSearchResultItem;
    setIsDropdownOpen: (isOpen: boolean) => void;
    setActiveItem: (item: StudioSearchResultItem) => void;
    setSearchQuery: React.Dispatch<React.SetStateAction<string>>;
    studioSearchInputId: string;
}

export function useStudioSearchKeyboardEvents(props: UseStudioSearchKeyboardEventsProps) {
    const { refs, results, activeItem, setIsDropdownOpen, setActiveItem, setSearchQuery, studioSearchInputId } = props;

    const { inputRef, dropdownRef, serverColumnRef, databaseColumnRef } = refs;

    // Handle toggle dropdown by keyboard
    useEffect(() => {
        const handleToggleDropdown = (e: KeyboardEvent) => {
            if (e.ctrlKey && e.key.toLowerCase() === "k") {
                e.preventDefault();
                setIsDropdownOpen(true);
                inputRef.current?.focus();
            }
            if (e.key === "Escape") {
                e.preventDefault();
                setIsDropdownOpen(false);
                inputRef.current?.blur();
            }
        };

        document.addEventListener("keydown", handleToggleDropdown);
        return () => {
            document.removeEventListener("keydown", handleToggleDropdown);
        };
    }, [inputRef, setIsDropdownOpen]);

    // Handle enter key
    useEffect(() => {
        const handleEnterKey = (e: KeyboardEvent) => {
            if (e.key === "Enter") {
                e.preventDefault();
                if (activeItem) {
                    activeItem.onSelected(e);
                }
            }
        };

        document.addEventListener("keydown", handleEnterKey);
        return () => {
            document.removeEventListener("keydown", handleEnterKey);
        };
    }, [activeItem, setIsDropdownOpen]);

    // Handle keyboard navigation
    useEffect(() => {
        let isFirstRun = true;
        let activeLeftIndex = 0;
        let activeRightIndex = 0;

        const rightFlatItems = results.server;
        const leftFlatItems = Object.values(results.database).flat().concat(results.switchToDatabase);

        let activeGroup: "left" | "right" = leftFlatItems.length > 0 ? "left" : "right";

        const handleKeyboardNavigation = (e: KeyboardEvent) => {
            if (e.key === "ArrowDown" && !isFirstRun) {
                e.preventDefault();
                if (activeGroup === "left") {
                    activeLeftIndex = (activeLeftIndex + 1) % leftFlatItems.length;
                } else {
                    activeRightIndex = (activeRightIndex + 1) % rightFlatItems.length;
                }
            }
            if (e.key === "ArrowUp" && !isFirstRun) {
                e.preventDefault();
                if (activeGroup === "left") {
                    activeLeftIndex = (activeLeftIndex - 1 + leftFlatItems.length) % leftFlatItems.length;
                } else {
                    activeRightIndex = (activeRightIndex - 1 + rightFlatItems.length) % rightFlatItems.length;
                }
            }
            if (e.altKey && e.key === "ArrowLeft") {
                e.preventDefault();
                activeGroup = "left";
            }
            if (e.altKey && e.key === "ArrowRight") {
                e.preventDefault();
                activeGroup = "right";
            }

            const flatItems = activeGroup === "left" ? leftFlatItems : rightFlatItems;
            const index = activeGroup === "left" ? activeLeftIndex : activeRightIndex;

            const newActiveItem = flatItems[index];
            setActiveItem(newActiveItem);
            isFirstRun = false;
        };

        const current = inputRef.current;
        current.addEventListener("keydown", handleKeyboardNavigation);

        return () => {
            current.removeEventListener("keydown", handleKeyboardNavigation);
        };
    }, [inputRef, serverColumnRef, results, setActiveItem]);

    // Handle scroll on active item change
    useEffect(() => {
        const activeElement = document.getElementById(activeItem?.id);
        if (!activeElement) {
            return;
        }

        const columnElement =
            activeItem.type === "serverMenuItem" ? serverColumnRef.current : databaseColumnRef.current;

        const activeElementPage = getScrollPageNumber(
            activeElement.offsetTop + activeElement.clientHeight,
            columnElement.clientHeight
        );

        const scrollToY = activeElementPage * columnElement.clientHeight - activeElement.clientHeight;

        columnElement.scrollTo(0, scrollToY);
    }, [activeItem?.id, activeItem?.type, databaseColumnRef, serverColumnRef]);

    // Prevent space from closing the dropdown
    useEffect(() => {
        dropdownRef.current.handleKeyDown = (e: KeyboardEvent) => {
            if (e.code === "Space") {
                e.preventDefault();

                if (document.activeElement?.id === studioSearchInputId) {
                    setSearchQuery((prev) => prev + " ");
                }
            }
        };
    }, [dropdownRef, setSearchQuery, studioSearchInputId]);
}

const getScrollPageNumber = (y: number, height: number): number => {
    return y === 0 ? 0 : Math.ceil(y / height) - 1;
};
