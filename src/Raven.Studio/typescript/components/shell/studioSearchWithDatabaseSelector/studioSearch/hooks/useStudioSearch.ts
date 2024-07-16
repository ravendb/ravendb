import { useState, useRef } from "react";
import useBoolean from "components/hooks/useBoolean";
import { StudioSearchResultDatabaseGroup, StudioSearchResultItem } from "../studioSearchTypes";
import { useStudioSearchAsyncRegister } from "./useStudioSearchAsyncRegister";
import { useStudioSearchKeyboardEvents } from "./useStudioSearchKeyboardEvents";
import { useStudioSearchSyncRegister } from "./useStudioSearchSyncRegister";
import { useStudioSearchOmniSearch } from "./useStudioSearchOmniSearch";
import { useStudioSearchUtils } from "./useStudioSearchUtils";

export function useStudioSearch(menuItems: menuItem[]) {
    const { value: isSearchDropdownOpen, setValue: setIsDropdownOpen } = useBoolean(false);

    const dropdownRef = useRef<any>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const serverColumnRef = useRef<HTMLDivElement>(null);
    const databaseColumnRef = useRef<HTMLDivElement>(null);

    const [searchQuery, setSearchQuery] = useState("");
    const [activeItem, setActiveItem] = useState<StudioSearchResultItem>(null);

    const { omniSearch, results, handleOmniSearch } = useStudioSearchOmniSearch(searchQuery);

    const refs = {
        inputRef,
        dropdownRef,
        serverColumnRef,
        databaseColumnRef,
    };

    const { toggleDropdown, goToUrl, resetDropdown } = useStudioSearchUtils({
        inputRef,
        studioSearchInputId,
        setIsDropdownOpen,
        setSearchQuery,
        setActiveItem,
    });

    useStudioSearchSyncRegister({
        omniSearch,
        menuItems,
        goToUrl,
        resetDropdown,
    });

    useStudioSearchAsyncRegister({
        omniSearch,
        searchQuery,
        goToUrl,
        handleOmniSearch,
    });

    useStudioSearchKeyboardEvents({
        refs,
        studioSearchInputId,
        results,
        activeItem,
        setIsDropdownOpen,
        setActiveItem,
        setSearchQuery,
    });

    const matchStatus = {
        hasServerMatch: results.server.length > 0,
        hasSwitchToDatabaseMatch: results.switchToDatabase.length > 0,
        hasDatabaseMatch: Object.keys(results.database).some(
            (groupType: StudioSearchResultDatabaseGroup) => results.database[groupType].length > 0
        ),
    };

    return {
        refs,
        isSearchDropdownOpen,
        toggleDropdown,
        searchQuery,
        setSearchQuery,
        matchStatus,
        results,
        activeItem,
    };
}

export const studioSearchInputId = "studio-search-input";
