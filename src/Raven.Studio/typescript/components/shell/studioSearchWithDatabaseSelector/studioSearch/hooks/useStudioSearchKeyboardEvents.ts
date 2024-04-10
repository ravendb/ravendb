import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import { useEffect } from "react";

interface UseStudioSearchKeyboardEventsProps {
    inputRef: React.RefObject<HTMLInputElement>;
    results: StudioSearchResult;
    dropdownRef: React.RefObject<any>;
    activeItem: StudioSearchResultItem;
    setIsDropdownOpen: (isOpen: boolean) => void;
    setActiveItem: (item: StudioSearchResultItem) => void;
    setSearchQuery: React.Dispatch<React.SetStateAction<string>>;
    studioSearchInputId: string;
}

export function useStudioSearchKeyboardEvents(props: UseStudioSearchKeyboardEventsProps) {
    const {
        inputRef,
        results,
        dropdownRef,
        activeItem,
        setIsDropdownOpen,
        setActiveItem,
        setSearchQuery,
        studioSearchInputId,
    } = props;

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
            if (!e.key.startsWith("Arrow")) {
                return;
            }

            e.preventDefault();

            if (e.key === "ArrowDown" && !isFirstRun) {
                if (activeGroup === "left") {
                    activeLeftIndex = (activeLeftIndex + 1) % leftFlatItems.length;
                } else {
                    activeRightIndex = (activeRightIndex + 1) % rightFlatItems.length;
                }
            }
            if (e.key === "ArrowUp" && !isFirstRun) {
                if (activeGroup === "left") {
                    activeLeftIndex = (activeLeftIndex - 1 + leftFlatItems.length) % leftFlatItems.length;
                } else {
                    activeRightIndex = (activeRightIndex - 1 + rightFlatItems.length) % rightFlatItems.length;
                }
            }
            if (e.altKey && e.key === "ArrowLeft") {
                activeGroup = "left";
            }
            if (e.altKey && e.key === "ArrowRight") {
                activeGroup = "right";
            }

            const flatItems = activeGroup === "left" ? leftFlatItems : rightFlatItems;
            const index = activeGroup === "left" ? activeLeftIndex : activeRightIndex;

            const activeItem = flatItems[index];
            setActiveItem(activeItem);
            isFirstRun = false;
        };

        const current = inputRef.current;
        current.addEventListener("keydown", handleKeyboardNavigation);

        return () => {
            current.removeEventListener("keydown", handleKeyboardNavigation);
        };
    }, [inputRef, results, setActiveItem]);

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
