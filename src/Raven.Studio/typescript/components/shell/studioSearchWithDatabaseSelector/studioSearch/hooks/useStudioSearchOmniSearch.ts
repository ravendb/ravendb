import { OmniSearch } from "common/omniSearch/omniSearch";
import {
    StudioSearchItem,
    StudioSearchItemType,
    StudioSearchResult,
} from "components/shell/studioSearchWithDatabaseSelector/studioSearch/studioSearchTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import { useState, useCallback, useEffect, useMemo } from "react";

export function useStudioSearchOmniSearch(searchQuery: string) {
    const omniSearch = useMemo(() => new OmniSearch<StudioSearchItem, StudioSearchItemType>(), []);

    const [results, setResults] = useState<StudioSearchResult>(emptyResult);

    const handleOmniSearch = useCallback(() => {
        const searchResults = omniSearch.search(searchQuery.trim());
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
    }, [omniSearch, searchQuery]);

    useEffect(() => {
        handleOmniSearch();
    }, [handleOmniSearch]);

    return {
        omniSearch,
        results,
        handleOmniSearch,
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
