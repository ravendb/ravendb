import type { RefObject } from "react";
import { useEffect, useState } from "react";
import { useIsMounted } from "hooks/useIsMounted";

type Size = {
    width: number | undefined;
    height: number | undefined;
};

type UseResizeObserverOptions<T extends HTMLElement = HTMLElement> = {
    ref: RefObject<T>;
    box?: "border-box" | "content-box" | "device-pixel-content-box";
};

export function useResizeObserver<T extends HTMLElement = HTMLElement>(options: UseResizeObserverOptions<T>): Size {
    const { ref, box = "content-box" } = options;
    const [width, setWidth] = useState<number>();
    const [height, setHeight] = useState<number>();

    const isMounted = useIsMounted();

    useEffect(() => {
        if (!ref.current || !isMounted()) {
            return;
        }

        const clientRect = ref.current.getBoundingClientRect();

        if (clientRect?.width) {
            setWidth(clientRect.width);
        }
        if (clientRect?.height) {
            setHeight(clientRect.height);
        }
    }, [isMounted]);

    useEffect(() => {
        if (!ref.current || !isMounted()) {
            return;
        }

        if (typeof window === "undefined" || !("ResizeObserver" in window)) {
            console.warn("ResizeObserver not supported");
            return;
        }

        const observer = new ResizeObserver(([entry]) => {
            const newWidth = extractSize(entry, box, "inlineSize");
            const newHeight = extractSize(entry, box, "blockSize");

            if (newWidth) {
                setWidth(newWidth);
            }
            if (newHeight) {
                setHeight(newHeight);
            }
        });

        observer.observe(ref.current, { box });

        return () => {
            observer.disconnect();
        };
    }, [box, isMounted]);

    return { width, height };
}

function extractSize(
    entry: ResizeObserverEntry,
    box: ResizeObserverBoxOptions,
    sizeType: keyof ResizeObserverSize
): number | undefined {
    const boxKey = getBoxKey(box);

    if (!entry[boxKey]) {
        if (boxKey === "contentBoxSize") {
            return entry.contentRect[sizeType === "inlineSize" ? "width" : "height"];
        }
        return undefined;
    }

    return Array.isArray(entry[boxKey])
        ? entry[boxKey][0][sizeType]
        : // @ts-expect-error Support Firefox's non-standard behavior
          (entry[box][sizeType] as number);
}

type BoxSizesKey = keyof Pick<ResizeObserverEntry, "borderBoxSize" | "contentBoxSize" | "devicePixelContentBoxSize">;

function getBoxKey(box: ResizeObserverBoxOptions): BoxSizesKey {
    switch (box) {
        case "border-box":
            return "borderBoxSize";
        case "device-pixel-content-box":
            return "devicePixelContentBoxSize";
        case "content-box":
            return "contentBoxSize";
        default:
            return "contentBoxSize";
    }
}
