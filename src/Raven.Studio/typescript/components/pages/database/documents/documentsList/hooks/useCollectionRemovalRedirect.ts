import appUrl from "common/appUrl";
import messagePublisher from "common/messagePublisher";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import router from "plugins/router";
import { useEffect, useRef } from "react";

export function useCollectionRemovalRedirect(databaseName: string, collectionName: string | null) {
    const collectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const wasCollectionSeenRef = useRef(false);
    const isRemovalExpectedRef = useRef(false);

    useEffect(() => {
        if (collectionName === null) {
            return;
        }

        if (collectionNames.includes(collectionName)) {
            wasCollectionSeenRef.current = true;
            return;
        }

        if (wasCollectionSeenRef.current && !isRemovalExpectedRef.current) {
            messagePublisher.reportWarning(`${collectionName} was removed`);
        }

        redirectToAllDocuments(databaseName);
    }, [collectionName, collectionNames, databaseName]);

    return {
        onCollectionDeletionStarted: () => {
            isRemovalExpectedRef.current = true;
        },
        onCollectionDeletionFailed: () => {
            isRemovalExpectedRef.current = false;
        },
    };
}

function redirectToAllDocuments(databaseName: string) {
    router.navigate(appUrl.forDocuments(null, databaseName), { replace: true, trigger: true });
}

export type CollectionDeletionCallbacks = ReturnType<typeof useCollectionRemovalRedirect>;
