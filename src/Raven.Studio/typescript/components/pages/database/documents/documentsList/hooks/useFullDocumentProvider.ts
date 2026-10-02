import getDocumentsPreviewCommand from "commands/database/documents/getDocumentsPreviewCommand";
import { useServices } from "components/hooks/useServices";
import document from "models/database/documents/document";
import { useCallback, useRef } from "react";

export function useFullDocumentProvider(databaseName: string) {
    const { databasesService } = useServices();
    const cacheRef = useRef(new Map<string, Promise<document>>());

    const getPropertyPreviewResolver = useCallback(
        (doc: document, property: string): (() => Promise<unknown>) | undefined => {
            if (!hasIncompletePreviewValue(doc, property)) {
                return undefined;
            }

            return async () => {
                const id = doc.getId();

                if (!cacheRef.current.has(id)) {
                    const fetched = databasesService.getDocumentWithMetadata(id, databaseName, true).then((result) => {
                        if (!result) {
                            throw new Error("The document no longer exists");
                        }
                        return result;
                    });
                    fetched.catch(() => cacheRef.current.delete(id));
                    cacheRef.current.set(id, fetched);
                }

                const fullDocument = await cacheRef.current.get(id);
                return fullDocument.getValue(property);
            };
        },
        [databasesService, databaseName]
    );

    return { getPropertyPreviewResolver, clearCache: () => cacheRef.current.clear() };
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
