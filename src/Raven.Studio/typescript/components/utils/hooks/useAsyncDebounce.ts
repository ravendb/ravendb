import { useMemo } from "react";
import { UseAsyncReturn, useAsync } from "react-async-hook";

export function useAsyncDebounce<ReturnType, ParamsType extends unknown[]>(
    callback: (...args: unknown[]) => Promise<ReturnType>,
    params: ParamsType,
    waitTimeMs = 500
): UseAsyncReturn<ReturnType, ParamsType> {
    // debounce should be created only once
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const debounced = useMemo(() => _.debounce(callback, waitTimeMs), []);

    return useAsync(debounced, params);
}
