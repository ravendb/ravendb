import { useAsync, useAsyncCallback } from "react-async-hook";
import React from "react";
import { useServices } from "hooks/useServices";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import recentError from "common/notifications/models/recentError";
import genUtils from "common/generalUtils";
import { useEventsCollector } from "hooks/useEventsCollector";
import { useAppUrls } from "hooks/useAppUrls";
import { getTermsFields, getTermsLoadedAmount } from "components/pages/database/indexes/list/terms/termsUtils";

export type termsForField = {
    name: string;
    terms: string[];
    fromValue: string;
    type: fieldType;
    hasMoreTerms: boolean;
    loadInProgress: boolean;
    loadError: string;
};

export type fieldType = "static" | "dynamic";

export const INDEX_TERMS_PAGE_LIMIT = 500;

export function useIndexTerms(indexName: string) {
    const { forCurrentDatabase: urls } = useAppUrls();
    const [indexTerms, setIndexTerms] = React.useState<termsForField[]>([]);
    const { indexesService } = useServices();
    const activeDb = useAppSelector(databaseSelectors.activeDatabase);
    const locations = DatabaseUtils.getLocations(activeDb);
    const { reportEvent } = useEventsCollector();
    const editUrl = urls.editIndex(indexName)();

    const asyncGetIndexEntriesFields = useAsync(
        async () => {
            const tasks = locations.map((location) =>
                indexesService.getIndexEntriesFields(indexName, activeDb.name, location)
            );

            const perNodeFields = await Promise.all(tasks);
            const termFields = getTermsFields(perNodeFields);

            return await Promise.all(termFields.map((field) => loadTerms.execute(indexName, field)));
        },
        [],
        {
            onSuccess: setIndexTerms,
        }
    );

    const loadTerms = useAsyncCallback<termsForField>(async (indexName: string, termsForField) => {
        try {
            const indexTerms = await indexesService.getIndexTerms(
                indexName,
                null,
                termsForField.name,
                activeDb.name,
                INDEX_TERMS_PAGE_LIMIT + 1,
                termsForField.fromValue
            );

            let loadedTerms = indexTerms.Terms;

            if (loadedTerms.length > INDEX_TERMS_PAGE_LIMIT) {
                termsForField.hasMoreTerms = true;
                loadedTerms = loadedTerms.slice(0, INDEX_TERMS_PAGE_LIMIT);
            } else {
                termsForField.hasMoreTerms = false;
            }

            termsForField.terms.push(...loadedTerms);

            if (loadedTerms.length > 0) {
                termsForField.fromValue = loadedTerms.at(-1);
            }

            return termsForField;
        } catch (e) {
            termsForField.hasMoreTerms = false;

            const messageAndOptionalException = recentError.tryExtractMessageAndException(e.responseText);
            termsForField.loadError = genUtils.trimMessage(messageAndOptionalException.message);

            return termsForField;
        }
    });

    const loadMore = useAsyncCallback(async (fieldName: string) => {
        reportEvent("terms", "load-more");

        const field = indexTerms.find((x) => x.name === fieldName);

        if (!field || !field.hasMoreTerms) {
            return;
        }

        field.loadInProgress = true;

        const terms = await loadTerms.execute(indexName, field);

        field.loadInProgress = false;

        setIndexTerms((prev) => {
            return prev.map((x) => (x.name === fieldName ? terms : x));
        });
        return field;
    });

    const termsLoadedAmount = getTermsLoadedAmount(indexTerms);

    return {
        asyncGetIndexEntriesFields,
        indexTerms,
        termsLoadedAmount,
        loadMore,
        editUrl,
    };
}
