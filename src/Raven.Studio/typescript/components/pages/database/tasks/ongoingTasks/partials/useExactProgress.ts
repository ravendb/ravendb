import { useEffect, useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import messagePublisher from "common/messagePublisher";

// the exact progress is shown until the next periodic refresh replaces it
export function useExactProgress<T>(progress: T, fetchExactProgress: () => Promise<T>) {
    const [exactProgress, setExactProgress] = useState<T>(null);

    useEffect(() => {
        setExactProgress(null);
    }, [progress]);

    const asyncCalculateExact = useAsyncCallback(async () => {
        try {
            setExactProgress(await fetchExactProgress());
        } catch (e) {
            messagePublisher.reportError("Failed to calculate the exact progress", e.responseText);
        }
    });

    const calculateExact = () => {
        if (!asyncCalculateExact.loading) {
            asyncCalculateExact.execute();
        }
    };

    return {
        progress: exactProgress ?? progress,
        calculateExact,
        isCalculatingExact: asyncCalculateExact.loading,
    };
}
