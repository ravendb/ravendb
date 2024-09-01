import AwesomeDebouncePromise from "awesome-debounce-promise";
import { useMemo } from "react";
import { UseAsyncOptionsNormalized, UseAsyncReturn, useAsync } from "react-async-hook";

export function useAsyncDebounce<ReturnType, const ParamsType extends unknown[]>(
    callback: (...args: ParamsType) => Promise<ReturnType>,
    params: ParamsType,
    waitTimeMs = 500,
    options: Partial<UseAsyncOptionsNormalized<ReturnType>> = null
): UseAsyncReturn<ReturnType, ParamsType> {
    // debounce should be created only once
    // eslint-disable-next-line react-hooks/exhaustive-deps
    const debounced = useMemo(() => AwesomeDebouncePromise(callback, waitTimeMs), []);

    return useAsync(debounced, params, options);
}
