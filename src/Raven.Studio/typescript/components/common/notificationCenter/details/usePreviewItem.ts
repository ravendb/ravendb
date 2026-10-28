import { useCallback, useState } from "react";

export function usePreviewItem<T>(items: T[], getKey: (item: T) => string): [T, (item: T) => void] {
    const [previewKey, setPreviewKey] = useState<string>(null);

    const previewItem = items.find((item) => getKey(item) === previewKey) ?? null;
    const showPreview = useCallback((item: T) => setPreviewKey(getKey(item)), [getKey]);

    return [previewItem, showPreview];
}
