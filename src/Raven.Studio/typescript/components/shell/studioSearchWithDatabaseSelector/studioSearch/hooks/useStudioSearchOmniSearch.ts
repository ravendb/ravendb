import { StudioSearchItem, StudioSearchItemType, StudioSearchResult } from "../studioSearchTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import { useState, useCallback, useEffect } from "react";
import { RangeTuple } from "fuse.js";
import { useOmniSearch } from "components/hooks/useOmniSearch";

interface Action {
    text: string;
    alternativeTexts?: string[];
}

export interface OmniSearchItem<TType = unknown> extends Action {
    type: TType;
    innerActions?: Action[];
}

export interface OmniSearchResults<TItem> {
    items: OmniSearchResultItem<TItem>[];
}

export interface OmniSearchResultItem<TItem> {
    item: TItem;
    indices?: readonly RangeTuple[];
    innerActionText?: string;
    innerActionIndices?: readonly RangeTuple[];
}

export function useStudioSearchOmniSearch(searchQuery: string) {
    const { register, search } = useOmniSearch<StudioSearchItem, StudioSearchItemType>();

    const [results, setResults] = useState<StudioSearchResult>(emptyResult);

    const handleOmniSearch = useCallback(() => {
        const searchResults = search(searchQuery.trim());
        const resultTypes = new Set(searchResults.items.map((x) => x.item.type));
        const newResult = JSON.parse(JSON.stringify(emptyResult));

        for (const resultType of resultTypes) {
            const resultsByType = searchResults.items.filter((x) => x.item.type === resultType);

            const items = resultsByType.map((x) => ({
                ...x.item,
                indices: x.indices,
                innerActionText: x.innerActionText,
                innerActionIndices: x.innerActionIndices,
                id: _.uniqueId("item-"),
            }));

            switch (resultType) {
                case "document":
                case "documentsMenuItem":
                    newResult.database.documents = items;
                    break;
                case "collection":
                    newResult.database.collections = items;
                    break;
                case "index":
                case "indexesMenuItem":
                    newResult.database.indexes = items;
                    break;
                case "task":
                case "tasksMenuItem":
                    newResult.database.tasks = items;
                    break;
                case "settingsMenuItem":
                    newResult.database.settings = items;
                    break;
                case "statsMenuItem":
                    newResult.database.stats = items;
                    break;
                case "serverMenuItem":
                    newResult.server = items;
                    break;
                case "database":
                    newResult.switchToDatabase = items;
                    break;
                default:
                    assertUnreachable(resultType);
            }
        }

        setResults(newResult);
    }, [search, searchQuery]);

    useEffect(() => {
        handleOmniSearch();
    }, [handleOmniSearch]);

    return {
        register,
        results,
    };
}

const emptyResult: StudioSearchResult = {
    server: [],
    database: {
        collections: [],
        documents: [],
        indexes: [],
        tasks: [],
        settings: [],
        stats: [],
    },
    switchToDatabase: [],
};
