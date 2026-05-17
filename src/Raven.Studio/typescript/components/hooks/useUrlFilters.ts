import { useCallback, useEffect, useRef, useState } from "react";
import router from "plugins/router";

export function useUrlFilters<T>(
    initialValues: T,
    buildUrl: (filters: T) => string,
    extraDeps: unknown[] = []
): [T, (patch: Partial<T>) => void] {
    const [filters, setFilters] = useState<T>(initialValues);

    const buildUrlRef = useRef(buildUrl);
    buildUrlRef.current = buildUrl;

    const filtersRef = useRef(filters);
    filtersRef.current = filters;

    useEffect(() => {
        const url = buildUrlRef.current(filtersRef.current);
        router.navigate(url, { trigger: false, replace: true });
    }, [filters, ...extraDeps]);

    const updateFilters = useCallback((patch: Partial<T>) => {
        setFilters((prev) => ({ ...prev, ...patch }));
    }, []);

    return [filters, updateFilters];
}
