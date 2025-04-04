import { debounce } from "lodash";
import { useMemo, useState } from "react";

export default function useDebouncedInput<T>({
    value,
    delayInMs = 300,
    onDebouncedUpdate,
}: {
    value: T;
    delayInMs?: number;
    onDebouncedUpdate: (value: T) => void;
}) {
    const [localValue, setLocalValue] = useState<T>(value);

    const debouncedUpdateValue = useMemo(
        () => debounce((value: T) => onDebouncedUpdate(value), delayInMs),
        // create only once
        []
    );

    return {
        localValue,
        handleChange: (value: T) => {
            setLocalValue(value);
            debouncedUpdateValue(value);
        },
    };
}
