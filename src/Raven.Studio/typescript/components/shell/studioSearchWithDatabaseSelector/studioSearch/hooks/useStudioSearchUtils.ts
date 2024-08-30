import { StudioSearchResultItem } from "../studioSearchTypes";
import { useCallback } from "react";
import router from "plugins/router";

interface UseStudioSearchUtilsProps {
    inputRef: React.RefObject<HTMLInputElement>;
    setIsDropdownOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setSearchQuery: React.Dispatch<React.SetStateAction<string>>;
    setActiveItem: React.Dispatch<React.SetStateAction<StudioSearchResultItem>>;
}

export function useStudioSearchUtils(props: UseStudioSearchUtilsProps) {
    const { inputRef, setIsDropdownOpen, setSearchQuery, setActiveItem } = props;

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
        goToUrl,
        resetDropdown,
    };
}
