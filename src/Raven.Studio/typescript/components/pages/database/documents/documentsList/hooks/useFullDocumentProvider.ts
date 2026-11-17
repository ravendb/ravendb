import getDocumentsPreviewCommand from "commands/database/documents/getDocumentsPreviewCommand";
import { DocumentPreviewSource } from "components/common/virtualTable/cells/CellWithDocumentPreview";
import { useServices } from "components/hooks/useServices";
import document from "models/database/documents/document";
import { useCallback, useRef } from "react";

const MAX_CACHED_DOCUMENTS = 50;

export function useFullDocumentProvider(databaseName: string) {
    const { databasesService } = useServices();
    const documentCacheRef = useRef(new Map<string, Promise<document | null>>());
    const revisionsCountCacheRef = useRef(new Map<string, Promise<number>>());

    const getFullDocument = useCallback(
        (id: string) =>
            fetchOnce(documentCacheRef.current, id, () =>
                databasesService.getDocumentWithMetadata(id, databaseName, true)
            ),
        [databasesService, databaseName]
    );

    const getRevisionsCount = useCallback(
        (id: string) =>
            fetchOnce(revisionsCountCacheRef.current, id, async () => {
                const { RevisionsCount } = await databasesService.getDocumentRevisionsCount(id, databaseName, false);
                return RevisionsCount;
            }),
        [databasesService, databaseName]
    );

    const getPropertyPreviewResolver = useCallback(
        (doc: document, property: string): (() => Promise<unknown>) | undefined => {
            if (!hasIncompletePreviewValue(doc, property)) {
                return undefined;
            }

            return async () => {
                const fullDocument = await getFullDocument(doc.getId());
                return fullDocument?.getValue(property);
            };
        },
        [getFullDocument]
    );

    const getDocumentPreviewSource = useCallback(
        (doc: document): DocumentPreviewSource => {
            const id = doc.getId();
            const hasRevisions = doc.__metadata.hasFlag("HasRevisions");

            return {
                load: async () => {
                    const [fullDocument, revisionsCount] = await Promise.all([
                        getFullDocument(id),
                        hasRevisions ? getRevisionsCount(id).catch(() => 0) : 0,
                    ]);

                    return fullDocument ? { document: fullDocument, revisionsCount } : null;
                },
            };
        },
        [getFullDocument, getRevisionsCount]
    );

    const clearCache = () => {
        documentCacheRef.current.clear();
        revisionsCountCacheRef.current.clear();
    };

    return { getPropertyPreviewResolver, getDocumentPreviewSource, clearCache };
}

function fetchOnce<T>(cache: Map<string, Promise<T>>, id: string, fetch: () => Promise<T>): Promise<T> {
    const fetched = cache.get(id) ?? fetchAndDropFailures(cache, id, fetch);

    cache.delete(id);
    cache.set(id, fetched);

    if (cache.size > MAX_CACHED_DOCUMENTS) {
        const [leastRecentId] = cache.keys();
        cache.delete(leastRecentId);
    }

    return fetched;
}

function fetchAndDropFailures<T>(cache: Map<string, Promise<T>>, id: string, fetch: () => Promise<T>): Promise<T> {
    const fetched = fetch();
    fetched.then(
        (result) => {
            if (result == null) {
                cache.delete(id);
            }
        },
        () => cache.delete(id)
    );
    return fetched;
}

function hasIncompletePreviewValue(doc: document, property: string): boolean {
    const metadata = doc.__metadata as unknown as Record<string, object | undefined>;
    const { ObjectStubsKey, ArrayStubsKey, TrimmedValueKey } = getDocumentsPreviewCommand;

    return (
        property in (metadata[ObjectStubsKey] ?? {}) ||
        property in (metadata[ArrayStubsKey] ?? {}) ||
        ((metadata[TrimmedValueKey] as string[]) ?? []).includes(property)
    );
}

export type FullDocumentProvider = ReturnType<typeof useFullDocumentProvider>;
