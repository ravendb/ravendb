import type { StateCreator } from "zustand/vanilla";
import { create as createUntracked } from "zustand/react";

export * from "zustand/vanilla";
export * from "zustand/react";

const storeResets: (() => void)[] = [];

function createTracked<T>(initializer: StateCreator<T>) {
    const store = createUntracked(initializer);
    storeResets.push(() => store.setState(store.getInitialState(), true));
    return store;
}

// Handles both create(initializer) and the curried create<T>()(initializer).
export const create = (<T>(initializer?: StateCreator<T>) =>
    initializer ? createTracked(initializer) : createTracked) as typeof createUntracked;

export function resetAllStores() {
    for (const reset of storeResets) {
        reset();
    }
}
