import { systemCollectionNames, collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import { useEffect, useRef, useState } from "react";

// The preview endpoint returns an ETag built as "<change vector>/<document count>" of the previewed collection
// (the database change vector for all documents). The data is considered changed when the ETag differs between
// the fetches of the same load, or when the collection stats notification brings a state other than the loaded one.
export function useDocumentsDataChanged(collectionName: string | null) {
    const collection = useAppSelector(
        collectionsTrackerSelectors.collectionByName(collectionName ?? systemCollectionNames.allDocuments)
    );
    const globalChangeVector = useAppSelector(collectionsTrackerSelectors.globalChangeVector);

    const changeVector = collectionName === null ? globalChangeVector : collection?.lastDocumentChangeVector;
    const expectedEtag = collection ? `${changeVector}/${collection.documentCount}` : null;

    const [isDataChanged, setIsDataChanged] = useState(false);

    const expectedEtagRef = useRef(expectedEtag);
    expectedEtagRef.current = expectedEtag;
    const trackedEtagRef = useRef(expectedEtag);
    const resultEtagRef = useRef<string>(null);

    useEffect(() => {
        if (expectedEtag === trackedEtagRef.current) {
            return;
        }

        trackedEtagRef.current = expectedEtag;

        if (resultEtagRef.current !== null && expectedEtag !== resultEtagRef.current) {
            setIsDataChanged(true);
        }
    }, [expectedEtag]);

    const trackResultEtag = (resultEtag: string | undefined) => {
        if (!resultEtag) {
            return;
        }

        if (resultEtagRef.current !== null && resultEtagRef.current !== resultEtag) {
            setIsDataChanged(true);
        }

        resultEtagRef.current = resultEtag;
    };

    const reset = () => {
        trackedEtagRef.current = expectedEtagRef.current;
        resultEtagRef.current = null;
        setIsDataChanged(false);
    };

    return { isDataChanged, trackResultEtag, reset };
}
