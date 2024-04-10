import { StudioSearchResultItem } from "../studioSearchTypes";
import { useCallback } from "react";
import router from "plugins/router";

interface UseStudioSearchUtilsProps {
    inputRef: React.RefObject<HTMLInputElement>;
    studioSearchInputId: string;
    setIsDropdownOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setSearchQuery: React.Dispatch<React.SetStateAction<string>>;
    setActiveItem: React.Dispatch<React.SetStateAction<StudioSearchResultItem>>;
}

export function useStudioSearchUtils(props: UseStudioSearchUtilsProps) {
    const { inputRef, studioSearchInputId, setIsDropdownOpen, setSearchQuery, setActiveItem } = props;

    const toggleDropdown = (e: any) => {
        setIsDropdownOpen((e.target as Element)?.id === studioSearchInputId);
    };

    const resetDropdown = useCallback(() => {
        inputRef.current?.blur();
        setIsDropdownOpen(false);
        setSearchQuery("");
        setActiveItem(null);
    }, [inputRef, setActiveItem, setIsDropdownOpen, setSearchQuery]);

    const goToUrl = useCallback(
        (url: string, newTab: boolean) => {
            resetDropdown();

            if (newTab) {
                window.open(url, "_blank").focus();
            } else {
                router.navigate(url);
            }
        },
        [resetDropdown]
    );

    return {
        toggleDropdown,
        goToUrl,
        resetDropdown,
    };
}
