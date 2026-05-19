import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useCallback, useReducer } from "react";
import { nodeAwareLoadableData } from "components/models/common";
import assertUnreachable from "components/utils/assertUnreachable";
import { produce } from "immer";
import { useAsync } from "react-async-hook";

interface ClusterWideReducerState<T> {
    result: nodeAwareLoadableData<T>[];
}

export function useClusterWideAsync<T>(perNodeProvider: (nodeTag: string) => Promise<T>) {
    const nodeTags = useAppSelector(clusterSelectors.allNodeTags);

    const [state, dispatch] = useReducer(clusterWideReducer<T>, nodeTags, initReducer<T>);

    const handleNode = useCallback(
        async (nodeTag: string) => {
            try {
                const result = await perNodeProvider(nodeTag);
                dispatch({
                    type: "NodeDataLoaded",
                    nodeTag,
                    data: result,
                });
            } catch (error) {
                dispatch({
                    type: "NodeDataError",
                    nodeTag,
                    error,
                });
            }
        },
        [perNodeProvider]
    );

    const { execute } = useAsync(() => Promise.allSettled(nodeTags.map(handleNode)), [nodeTags, handleNode]);

    return {
        result: state.result,
        refresh: execute,
    };
}

export function initReducer<T>(nodeTags: string[]): ClusterWideReducerState<T> {
    return {
        result: nodeTags.map(
            (tag): nodeAwareLoadableData<T> => ({
                nodeTag: tag,
                data: undefined,
                status: "loading",
                error: undefined,
            })
        ),
    };
}

type ClusterWideReducerAction<T> = ActionNodeDataLoaded<T> | ActionNodeDataError;

interface ActionNodeDataLoaded<T> {
    nodeTag: string;
    type: "NodeDataLoaded";
    data: T;
}

interface ActionNodeDataError {
    nodeTag: string;
    type: "NodeDataError";
    error: any;
}

function clusterWideReducer<T>(
    state: ClusterWideReducerState<T>,
    action: ClusterWideReducerAction<T>
): ClusterWideReducerState<T> {
    const type = action.type;
    switch (type) {
        case "NodeDataLoaded":
            return produce(state, (draft) => {
                const itemToModify = draft.result.find((t) => t.nodeTag === action.nodeTag);
                if (!itemToModify) {
                    throw new Error("Unable to find data for node = " + action.nodeTag);
                }
                itemToModify.status = "success";
                itemToModify.data = action.data as any;
                itemToModify.error = undefined;
            });
        case "NodeDataError":
            return produce(state, (draft) => {
                const itemToModify = draft.result.find((t) => t.nodeTag === action.nodeTag);
                if (!itemToModify) {
                    throw new Error("Unable to find data for node = " + action.nodeTag);
                }
                itemToModify.status = "failure";
                itemToModify.data = undefined;
                itemToModify.error = action.error;
            });
        default:
            assertUnreachable(type);
    }
}
