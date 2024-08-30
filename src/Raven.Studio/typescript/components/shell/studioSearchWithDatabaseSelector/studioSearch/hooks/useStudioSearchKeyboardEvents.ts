import { StudioSearchResultItem, StudioSearchResult } from "../studioSearchTypes";
import { useEffect, useMemo, useState } from "react";

interface UseStudioSearchKeyboardEventsProps {
    refs: {
        inputRef: React.RefObject<HTMLInputElement>;
        dropdownRef: React.RefObject<any>;
        serverColumnRef: React.RefObject<HTMLDivElement>;
        databaseColumnRef: React.RefObject<HTMLDivElement>;
    };
    studioSearchInputId: string;
    results: StudioSearchResult;
    activeItem: StudioSearchResultItem;
    setActiveItem: React.Dispatch<React.SetStateAction<StudioSearchResultItem>>;
    setIsDropdownOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setSearchQuery: React.Dispatch<React.SetStateAction<string>>;
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

    // Handle closing dropdown on tab key
    useEffect(() => {
        const handleTabKey = (e: KeyboardEvent) => {
            if (e.key === "Tab") {
                setIsDropdownOpen(false);
            }
        };

        const current = inputRef.current;
        current.addEventListener("keydown", handleTabKey);

        return () => {
            current.removeEventListener("keydown", handleTabKey);
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

        const current = inputRef.current;

        current.addEventListener("keydown", handleEnterKey);
        return () => {
            current.removeEventListener("keydown", handleEnterKey);
        };
    }, [inputRef, activeItem, setIsDropdownOpen]);

    const [activeGroup, setActiveGroup] = useState<"left" | "right">("left");
    const [activeIndex, setActiveIndex] = useState(0);

    const leftFlatItems = useMemo(
        () => Object.values(results.database).flat().concat(results.switchToDatabase),
        [results]
    );
    const rightFlatItems = useMemo(() => results.server, [results]);

    const leftFlatItemsLength = leftFlatItems.length;
    const rightFlatItemsLength = rightFlatItems.length;

    // Handle switching group when some list is empty
    useEffect(() => {
        if (leftFlatItemsLength > 0 && rightFlatItemsLength === 0) {
            setActiveGroup("left");
        }
        if (leftFlatItemsLength === 0 && rightFlatItemsLength > 0) {
            setActiveGroup("right");
        }
        if (leftFlatItemsLength === 0 && rightFlatItemsLength === 0) {
            setActiveGroup("left");
        }

        setActiveIndex(0);
    }, [leftFlatItemsLength, rightFlatItemsLength, setActiveIndex, setActiveGroup]);

    // Handle switching active item
    useEffect(() => {
        const newActiveItem = activeGroup === "left" ? leftFlatItems[activeIndex] : rightFlatItems[activeIndex];

        if (newActiveItem?.id !== activeItem?.id) {
            setActiveItem(newActiveItem);
        }
    }, [activeGroup, activeIndex, activeItem?.id, leftFlatItems, rightFlatItems, setActiveItem]);

    // Handle keyboard navigation
    useEffect(() => {
        const handleKeyboardNavigation = (e: KeyboardEvent) => {
            if (e.key === "ArrowDown") {
                e.preventDefault();
                if (activeGroup === "left") {
                    setActiveIndex((activeIndex + 1) % leftFlatItemsLength);
                } else {
                    setActiveIndex((activeIndex + 1) % rightFlatItemsLength);
                }
            }
            if (e.key === "ArrowUp") {
                e.preventDefault();
                if (activeGroup === "left") {
                    setActiveIndex((activeIndex - 1 + leftFlatItemsLength) % leftFlatItemsLength);
                } else {
                    setActiveIndex((activeIndex - 1 + rightFlatItemsLength) % rightFlatItemsLength);
                }
            }
            if (e.altKey && e.key === "ArrowLeft") {
                e.preventDefault();
                if (leftFlatItemsLength > 0) {
                    setActiveIndex(Math.min(activeIndex, leftFlatItemsLength - 1));
                    setActiveGroup("left");
                }
            }
            if (e.altKey && e.key === "ArrowRight") {
                e.preventDefault();
                if (rightFlatItemsLength > 0) {
                    setActiveIndex(Math.min(activeIndex, rightFlatItemsLength - 1));
                    setActiveGroup("right");
                }
            }
        };

        const current = inputRef.current;
        current.addEventListener("keydown", handleKeyboardNavigation);

        return () => {
            current.removeEventListener("keydown", handleKeyboardNavigation);
        };
    }, [activeIndex, activeGroup, inputRef, leftFlatItemsLength, rightFlatItemsLength]);

    // Handle scroll on active item change
    useEffect(() => {
        const activeItem = activeGroup === "left" ? leftFlatItems[activeIndex] : rightFlatItems[activeIndex];

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
    }, [activeGroup, activeIndex, databaseColumnRef, leftFlatItems, rightFlatItems, serverColumnRef]);

    // Prevent space from closing the dropdown (reactstrap issue)
    // https://github.com/reactstrap/reactstrap/issues/1945
    // Workaround
    const [cursorPosition, setCursorPosition] = useState([0, 0]);

    useEffect(() => {
        dropdownRef.current.handleKeyDown = (e: KeyboardEvent) => {
            setCursorPosition([inputRef.current.selectionStart, inputRef.current.selectionEnd]);

            if (e.code === "Space") {
                e.preventDefault();

                if (document.activeElement?.id === studioSearchInputId) {
                    setSearchQuery((prev) => {
                        const part1 = prev.substring(0, inputRef.current.selectionEnd);
                        const part2 = prev.substring(inputRef.current.selectionEnd);

                        return part1 + " " + part2;
                    });
                    setCursorPosition([inputRef.current.selectionStart + 1, inputRef.current.selectionEnd + 1]);
                }
            }
        };
    }, [inputRef, dropdownRef, setSearchQuery, studioSearchInputId]);

    useEffect(() => {
        inputRef.current.setSelectionRange(cursorPosition[0], cursorPosition[1]);
    }, [cursorPosition, inputRef]);
    // end of workaround
}

const getScrollPageNumber = (y: number, height: number): number => {
    return y === 0 ? 0 : Math.ceil(y / height) - 1;
};
