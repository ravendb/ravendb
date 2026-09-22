import { hasIncompletePreviewValue } from "components/common/virtualTable/utils/documentPreviewStubs";
import { useServices } from "components/hooks/useServices";
import document from "models/database/documents/document";
import { useCallback, useRef } from "react";

// The preview rows hold stubs for nested values and trimmed strings, this provides the full documents on demand
// (fetched once per id until the cache is cleared) so the cell previews can show the whole value.
export function useFullDocumentProvider(databaseName: string) {
    const { databasesService } = useServices();
    const cacheRef = useRef(new Map<string, Promise<document>>());

    const getFullDocument = useCallback(
        (id: string): Promise<document> => {
            const cached = cacheRef.current.get(id);
            if (cached) {
                return cached;
            }

            const fetched = databasesService.getDocumentWithMetadata(id, databaseName, true).catch((error) => {
                cacheRef.current.delete(id);
                throw error;
            });
            cacheRef.current.set(id, fetched);

            return fetched;
        },
        [databasesService, databaseName]
    );

    const getPropertyPreviewResolver = useCallback(
        (doc: document, property: string): (() => Promise<unknown>) | undefined => {
            if (!hasIncompletePreviewValue(doc, property)) {
                return undefined;
            }

            return () => getFullDocument(doc.getId()).then((fullDocument) => fullDocument.getValue(property));
        },
        [getFullDocument]
    );

    const getCustomColumnPreviewResolver = useCallback(
        (
            doc: document,
            properties: string[],
            evaluate: (doc: document) => unknown
        ): (() => Promise<unknown>) | undefined => {
            if (!properties.some((property) => hasIncompletePreviewValue(doc, property))) {
                return undefined;
            }

            return () => getFullDocument(doc.getId()).then(evaluate);
        },
        [getFullDocument]
    );

    const clearCache = useCallback(() => {
        cacheRef.current.clear();
    }, []);

    return { getPropertyPreviewResolver, getCustomColumnPreviewResolver, clearCache };
}

export type FullDocumentProvider = ReturnType<typeof useFullDocumentProvider>;
