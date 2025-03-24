import { useCallback, useEffect, useMemo } from "react";
import { UseAsyncOptionsNormalized, UseAsyncReturn, useAsyncCallback } from "react-async-hook";

export function useAsyncDebounce<T>(
    callback: () => Promise<T>,
    params: unknown[],
    waitTimeMs = 500,
    options: Partial<UseAsyncOptionsNormalized<T>> = null
): UseAsyncReturn<T, []> {
    // eslint-disable-next-line react-compiler/react-compiler
    const memorizedCallback = useCallback(callback, params);

    const asyncCallback = useAsyncCallback(memorizedCallback, options);

    const debounced = useMemo(() => _.debounce(asyncCallback.execute, waitTimeMs), []);

    useEffect(() => {
        asyncCallback.set({
            status: "loading",
            loading: true,
            error: undefined,
            result: undefined,
        });
        debounced();
    }, [memorizedCallback]);

    return asyncCallback;
}
