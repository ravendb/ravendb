import { Collection, collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { useAppSelector } from "components/store";
import { useEffect, useRef, useState } from "react";

interface UseDocumentsDataChangedProps {
    collection: Collection | null;
    isAllDocuments: boolean;
    isSharded: boolean;
    fetchCurrentEtag: () => Promise<string>;
}

export function useDocumentsDataChanged({
    collection,
    isAllDocuments,
    isSharded,
    fetchCurrentEtag,
}: UseDocumentsDataChangedProps) {
    const globalChangeVector = useAppSelector(collectionsTrackerSelectors.globalChangeVector);

    const changeVector = isAllDocuments ? globalChangeVector : collection?.lastDocumentChangeVector;
    const statsEtag = collection && changeVector ? `${changeVector}/${collection.documentCount}` : null;

    // Sharded stats only sum the document counts of the shards. The collection change vector comes from a single
    // shard and the global change vector is null, so an update on another shard may not change these stats at all.
    // Keying on the collection object, recreated on every stats notification, verifies each one on the server.
    const statsVersion: unknown = isSharded ? collection : statsEtag;

    const [isDataChanged, setIsDataChanged] = useState(false);
    const isDataChangedRef = useRef(false);
    const resultEtagRef = useRef<string>(null);
    const statsVersionAtResetRef = useRef(statsVersion);
    const resetCountRef = useRef(0);
    const isProbeRunningRef = useRef(false);
    const isProbeRerunRequestedRef = useRef(false);

    const markDataChanged = () => {
        isDataChangedRef.current = true;
        setIsDataChanged(true);
    };

    const probeCurrentEtag = async () => {
        if (isProbeRunningRef.current) {
            isProbeRerunRequestedRef.current = true;
            return;
        }

        isProbeRunningRef.current = true;

        do {
            isProbeRerunRequestedRef.current = false;
            const resetCount = resetCountRef.current;
            const currentEtag = await fetchCurrentEtag().catch((): string => null);

            const isSameLoad = resetCount === resetCountRef.current && resultEtagRef.current !== null;
            if (isSameLoad && currentEtag !== null && currentEtag !== resultEtagRef.current) {
                markDataChanged();
            }
        } while (isProbeRerunRequestedRef.current && !isDataChangedRef.current);

        isProbeRunningRef.current = false;
    };

    const verifyResultEtag = () => {
        if (isDataChangedRef.current || resultEtagRef.current === null) {
            return;
        }

        if (isSharded) {
            probeCurrentEtag();
        } else if (statsEtag !== null && statsEtag !== resultEtagRef.current) {
            markDataChanged();
        }
    };

    useEffect(() => {
        verifyResultEtag();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [statsVersion]);

    const trackResultEtag = (resultEtag: string) => {
        if (!resultEtag) {
            return;
        }

        const previousResultEtag = resultEtagRef.current;
        resultEtagRef.current = resultEtag;

        if (previousResultEtag !== null) {
            if (previousResultEtag !== resultEtag) {
                markDataChanged();
            }
            return;
        }

        if (statsVersion !== statsVersionAtResetRef.current) {
            verifyResultEtag();
        }
    };

    const reset = () => {
        resetCountRef.current++;
        resultEtagRef.current = null;
        statsVersionAtResetRef.current = statsVersion;
        isDataChangedRef.current = false;
        setIsDataChanged(false);
    };

    return { isDataChanged, trackResultEtag, reset };
}
