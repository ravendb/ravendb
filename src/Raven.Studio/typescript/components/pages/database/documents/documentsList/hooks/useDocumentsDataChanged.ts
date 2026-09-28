import { systemCollectionNames, collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import { useEffect, useRef, useState } from "react";

export function useDocumentsDataChanged(collectionName: string | null) {
    const collection = useAppSelector(
        collectionsTrackerSelectors.collectionByName(collectionName ?? systemCollectionNames.allDocuments)
    );
    const globalChangeVector = useAppSelector(collectionsTrackerSelectors.globalChangeVector);

    const changeVector = collectionName === null ? globalChangeVector : collection?.lastDocumentChangeVector;
    const expectedEtag = collection ? `${changeVector}/${collection.documentCount}` : null;

    const [isDataChanged, setIsDataChanged] = useState(false);
    const resultEtagRef = useRef<string>(null);

    // Flags the loaded rows as stale when a collection stats notification brings a different ETag
    useEffect(() => {
        if (resultEtagRef.current !== null && expectedEtag !== resultEtagRef.current) {
            setIsDataChanged(true);
        }
    }, [expectedEtag]);

    const trackResultEtag = (resultEtag: string) => {
        if (resultEtagRef.current !== null && resultEtagRef.current !== resultEtag) {
            setIsDataChanged(true);
        }

        resultEtagRef.current = resultEtag;
    };

    const reset = () => {
        resultEtagRef.current = null;
        setIsDataChanged(false);
    };

    return { isDataChanged, trackResultEtag, reset };
}
