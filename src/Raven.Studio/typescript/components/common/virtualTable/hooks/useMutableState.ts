import { startTransition, useRef, useState } from "react";

export interface MutableState<S extends object> {
    // snapshot for rendering
    value: S;
    // always holds the latest values, safe to read from event handlers and async continuations
    ref: React.MutableRefObject<S>;
    set: (changes: Partial<S>) => void;
    // the same as set, but the re-render may be delayed by React when it is busy
    setDeferred: (changes: Partial<S>) => void;
}

// State whose latest values have to be readable outside of a render (scroll handlers, pending fetches).
// It lives in a ref and is mirrored into React state so rendering still follows it.
export function useMutableState<S extends object>(createInitialValue: () => S): MutableState<S> {
    const ref = useRef<S>(null);
    if (ref.current === null) {
        ref.current = createInitialValue();
    }

    const [value, setValue] = useState(ref.current);

    const apply = (changes: Partial<S>, isDeferred: boolean) => {
        const changedKeys = Object.keys(changes) as (keyof S)[];
        if (changedKeys.every((key) => ref.current[key] === changes[key])) {
            return;
        }

        const nextValue = { ...ref.current, ...changes };
        ref.current = nextValue;

        if (isDeferred) {
            startTransition(() => setValue(nextValue));
        } else {
            setValue(nextValue);
        }
    };

    return {
        value,
        ref,
        set: (changes) => apply(changes, false),
        setDeferred: (changes) => apply(changes, true),
    };
}
