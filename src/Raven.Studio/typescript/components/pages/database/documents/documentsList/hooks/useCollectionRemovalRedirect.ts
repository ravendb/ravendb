import appUrl from "common/appUrl";
import messagePublisher from "common/messagePublisher";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import router from "plugins/router";
import { useEffect, useRef } from "react";

interface UseCollectionRemovalRedirectProps {
    databaseName: string;
    // null means all documents
    collectionName: string | null;
}

// Redirects to all documents once the shown collection disappears from the collections stats.
// The user is warned unless the removal was started from this view (deleting the whole collection).
export function useCollectionRemovalRedirect({ databaseName, collectionName }: UseCollectionRemovalRedirectProps) {
    const collectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const seenCollectionRef = useRef<string>(null);
    const expectedRemovalRef = useRef<string>(null);

    useEffect(() => {
        if (collectionName === null || collectionNames.length === 0) {
            return;
        }

        if (collectionNames.includes(collectionName)) {
            seenCollectionRef.current = collectionName;
            return;
        }

        const isExpectedRemoval = expectedRemovalRef.current === collectionName;
        if (isExpectedRemoval) {
            expectedRemovalRef.current = null;
        } else if (seenCollectionRef.current === collectionName) {
            messagePublisher.reportWarning(`${collectionName} was removed`);
        }

        redirectToAllDocuments(databaseName);
    }, [collectionName, collectionNames, databaseName]);

    const onCollectionDeletionStarted = (deletedCollectionName: string) => {
        expectedRemovalRef.current = deletedCollectionName;
    };

    const onCollectionDeletionFailed = (deletedCollectionName: string) => {
        if (expectedRemovalRef.current === deletedCollectionName) {
            expectedRemovalRef.current = null;
        }
    };

    const onEntireCollectionDeleted = (deletedCollectionName: string) => {
        if (deletedCollectionName === collectionName) {
            redirectToAllDocuments(databaseName);
        }
    };

    return { onCollectionDeletionStarted, onCollectionDeletionFailed, onEntireCollectionDeleted };
}

function redirectToAllDocuments(databaseName: string) {
    router.navigate(appUrl.forDocuments(null, databaseName));
}

export type CollectionDeletionCallbacks = ReturnType<typeof useCollectionRemovalRedirect>;
