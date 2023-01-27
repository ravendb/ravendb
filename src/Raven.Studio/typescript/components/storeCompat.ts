import store from "components/store";

let effectiveStore: typeof store = store;

export function setEffectiveTestStore(newStore: typeof store) {
    effectiveStore = newStore;
}

/**
 * @deprecated Escape hatch to allow non-react components to update redux store aside
 */
export const globalDispatch: typeof store.dispatch = (action: any) => effectiveStore.dispatch(action);
