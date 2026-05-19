import { useAppSelector } from "components/store";
import { useCallback, useEffect, useReducer } from "react";
import { locationAwareLoadableData } from "components/models/common";
import assertUnreachable from "components/utils/assertUnreachable";
import { produce } from "immer";
import { useAsync } from "react-async-hook";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { databaseLocationComparator } from "components/utils/common";

interface DatabaseWideReducerState<T> {
    result: locationAwareLoadableData<T>[];
}

function locationsEqual(a: databaseLocationSpecifier[], b: databaseLocationSpecifier[]): boolean {
    return a.length === b.length && a.every((loc, i) => databaseLocationComparator(loc, b[i]));
}

export function useDatabaseWideAsync<T>(perNodeProvider: (location: databaseLocationSpecifier) => Promise<T>) {
    const locations = useAppSelector(
        (store) => DatabaseUtils.getLocations(databaseSelectors.activeDatabase(store)),
        locationsEqual
    );

    const [state, dispatch] = useReducer(databaseWideReducer<T>, locations, initReducer<T>);

    const handleLocation = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const result = await perNodeProvider(location);
                dispatch({ type: "LocationDataLoaded", location, data: result });
            } catch (error) {
                dispatch({ type: "LocationDataError", location, error });
            }
        },
        [perNodeProvider]
    );

    useEffect(() => {
        dispatch({ type: "Reset", locations });
    }, [locations]);

    const { execute, loading } = useAsync(
        () => Promise.allSettled(locations.map(handleLocation)),
        [locations, handleLocation]
    );

    return {
        result: state.result,
        refresh: execute,
        loading,
    };
}

function initReducer<T>(locations: databaseLocationSpecifier[]): DatabaseWideReducerState<T> {
    return {
        result: locations.map(
            (location): locationAwareLoadableData<T> => ({
                location,
                data: undefined,
                status: "loading",
                error: undefined,
            })
        ),
    };
}

interface ActionReset {
    type: "Reset";
    locations: databaseLocationSpecifier[];
}

type DatabaseWideReducerAction<T> = ActionLocationDataLoaded<T> | ActionLocationDataError | ActionReset;

interface ActionLocationDataLoaded<T> {
    location: databaseLocationSpecifier;
    type: "LocationDataLoaded";
    data: T;
}

interface ActionLocationDataError {
    location: databaseLocationSpecifier;
    type: "LocationDataError";
    error: any;
}

function databaseWideReducer<T>(
    state: DatabaseWideReducerState<T>,
    action: DatabaseWideReducerAction<T>
): DatabaseWideReducerState<T> {
    const type = action.type;
    switch (type) {
        case "LocationDataLoaded":
            return produce(state, (draft) => {
                const itemToModify = draft.result.find((t) => databaseLocationComparator(t.location, action.location));
                if (!itemToModify) {
                    // Stale response from a previous database — ignore it
                    return;
                }
                itemToModify.status = "success";
                itemToModify.data = action.data as any;
                itemToModify.error = undefined;
            });
        case "LocationDataError":
            return produce(state, (draft) => {
                const itemToModify = draft.result.find((t) => databaseLocationComparator(t.location, action.location));
                if (!itemToModify) {
                    // Stale response from a previous database — ignore it
                    return;
                }
                itemToModify.status = "failure";
                itemToModify.data = undefined;
                itemToModify.error = action.error;
            });
        case "Reset":
            return initReducer(action.locations);
        default:
            assertUnreachable(type);
    }
}
