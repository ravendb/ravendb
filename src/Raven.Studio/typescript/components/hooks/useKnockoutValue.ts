import { useCallback, useRef, useSyncExternalStore } from "react";

type Subscribable<T> = KnockoutObservable<T> | KnockoutComputed<T>;

interface Snapshot<T> {
    subscribable: Subscribable<T>;
    changeCount: number;
    value: T;
}

export function useKnockoutValue<T>(subscribable: Subscribable<T>): T {
    const changeCountRef = useRef(0);
    const snapshotRef = useRef<Snapshot<T>>(null);

    const subscribe = useCallback(
        (onChange: () => void) => {
            const subscription = subscribable.subscribe(() => {
                changeCountRef.current++;
                onChange();
            });
            return () => subscription.dispose();
        },
        [subscribable]
    );

    const getSnapshot = useCallback(() => {
        const value = subscribable.peek();
        const snapshot = snapshotRef.current;

        const isSnapshotCurrent =
            snapshot?.subscribable === subscribable &&
            snapshot.changeCount === changeCountRef.current &&
            isSameValue(snapshot.value, value);

        if (!isSnapshotCurrent) {
            snapshotRef.current = { subscribable, changeCount: changeCountRef.current, value: copyArray(value) };
        }

        return snapshotRef.current.value;
    }, [subscribable]);

    return useSyncExternalStore(subscribe, getSnapshot);
}

function copyArray<T>(value: T): T {
    return Array.isArray(value) ? ([...value] as T) : value;
}

function isSameValue(snapshotValue: unknown, value: unknown): boolean {
    if (Array.isArray(snapshotValue) && Array.isArray(value)) {
        return snapshotValue.length === value.length && snapshotValue.every((item, index) => item === value[index]);
    }

    return Object.is(snapshotValue, value);
}
