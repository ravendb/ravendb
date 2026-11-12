import getDocumentsPreviewCommand from "commands/database/documents/getDocumentsPreviewCommand";
import { DocumentPreview, DocumentPreviewSource } from "components/common/virtualTable/cells/CellWithDocumentPreview";
import { useServices } from "components/hooks/useServices";
import document from "models/database/documents/document";
import { useCallback, useRef } from "react";

export function useFullDocumentProvider(databaseName: string) {
    const { databasesService } = useServices();
    const documentCacheRef = useRef(new Map<string, Promise<document | null>>());
    const previewCacheRef = useRef(new Map<string, Promise<DocumentPreview | null>>());
    const loadedPreviewsRef = useRef(new Map<string, DocumentPreview>());

    const getFullDocument = useCallback(
        (id: string) =>
            fetchOnce(documentCacheRef.current, id, () =>
                databasesService.getDocumentWithMetadata(id, databaseName, true)
            ),
        [databasesService, databaseName]
    );

    const getRevisionsCount = useCallback(
        async (id: string) => {
            const { RevisionsCount } = await databasesService.getDocumentRevisionsCount(id, databaseName);
            return RevisionsCount;
        },
        [databasesService, databaseName]
    );

    const getPropertyPreviewResolver = useCallback(
        (doc: document, property: string): (() => Promise<unknown>) | undefined => {
            if (!hasIncompletePreviewValue(doc, property)) {
                return undefined;
            }

            return async () => {
                const fullDocument = await getFullDocument(doc.getId());
                if (!fullDocument) {
                    throw new Error("The document no longer exists");
                }
                return fullDocument.getValue(property);
            };
        },
        [getFullDocument]
    );

    const getDocumentPreviewSource = useCallback(
        (doc: document): DocumentPreviewSource => {
            const id = doc.getId();
            const hasRevisions = doc.__metadata.hasFlag("HasRevisions");

            return {
                load: () =>
                    fetchOnce(previewCacheRef.current, id, async () => {
                        const [fullDocument, revisionsCount] = await Promise.all([
                            getFullDocument(id),
                            hasRevisions ? getRevisionsCount(id).catch(() => 0) : 0,
                        ]);

                        if (!fullDocument) {
                            return null;
                        }

                        const preview = { document: fullDocument, revisionsCount };
                        loadedPreviewsRef.current.set(id, preview);
                        return preview;
                    }),
                getLoaded: () => loadedPreviewsRef.current.get(id),
            };
        },
        [getFullDocument, getRevisionsCount]
    );

    const clearCache = () => {
        documentCacheRef.current.clear();
        previewCacheRef.current.clear();
        loadedPreviewsRef.current.clear();
    };

    return { getPropertyPreviewResolver, getDocumentPreviewSource, clearCache };
}

function fetchOnce<T>(cache: Map<string, Promise<T>>, id: string, fetch: () => Promise<T>): Promise<T> {
    if (!cache.has(id)) {
        const fetched = fetch();
        fetched.then(
            (result) => {
                if (result == null) {
                    cache.delete(id);
                }
            },
            () => cache.delete(id)
        );
        cache.set(id, fetched);
    }

    return cache.get(id);
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
