import { useState, useRef, useCallback } from "react";
import useBoolean from "components/hooks/useBoolean";
import { StudioSearchResultDatabaseGroup, StudioSearchResultItem } from "../studioSearchTypes";
import { useStudioSearchAsyncRegister } from "./useStudioSearchAsyncRegister";
import { useStudioSearchKeyboardEvents } from "./useStudioSearchKeyboardEvents";
import { useStudioSearchSyncRegister } from "./useStudioSearchSyncRegister";
import { useStudioSearchOmniSearch } from "./useStudioSearchOmniSearch";
import { useStudioSearchUtils } from "./useStudioSearchUtils";
import { useStudioSearchMouseEvents } from "./useStudioSearchMouseEvents";
import { chatbotActions } from "components/shell/chatbot/store/chatbotSlice";
import { useAppDispatch } from "components/store";

export function useStudioSearch(menuItems: menuItem[]) {
    const { value: isSearchDropdownOpen, setValue: setIsDropdownOpen } = useBoolean(false);

    const inputRef = useRef<HTMLInputElement>(null);

    const serverColumnRef = useRef<HTMLDivElement>(null);
    const databaseColumnRef = useRef<HTMLDivElement>(null);

    const [searchQuery, setSearchQuery] = useState("");
    const [activeItem, setActiveItem] = useState<StudioSearchResultItem>(null);

    const { register, results } = useStudioSearchOmniSearch(searchQuery);

    const dispatch = useAppDispatch();

    const handleAskAi = useCallback(() => {
        dispatch(chatbotActions.conversationIdSet(null));
        dispatch(chatbotActions.isOpenSet(true));
        dispatch(chatbotActions.runChat({ message: searchQuery }));
        setSearchQuery("");
        setIsDropdownOpen(false);
    }, [searchQuery]);

    const refs = {
        inputRef,
        serverColumnRef,
        databaseColumnRef,
    };

    const { goToUrl, resetDropdown } = useStudioSearchUtils({
        inputRef,
        setIsDropdownOpen,
        setSearchQuery,
        setActiveItem,
    });

    useStudioSearchSyncRegister({
        register,
        menuItems,
        goToUrl,
        resetDropdown,
    });

    useStudioSearchAsyncRegister({
        register,
        searchQuery,
        goToUrl,
    });

    useStudioSearchKeyboardEvents({
        refs,
        studioSearchInputId,
        results,
        activeItem,
        setIsDropdownOpen,
        setActiveItem,
        handleAskAi,
    });

    useStudioSearchMouseEvents({
        inputRef,
        studioSearchBackdropId,
        setIsDropdownOpen,
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
        setIsDropdownOpen,
        searchQuery,
        setSearchQuery,
        matchStatus,
        results,
        activeItem,
        handleAskAi,
    };
}

export const studioSearchInputId = "studio-search-input";
export const studioSearchBackdropId = "studio-search-backdrop";
