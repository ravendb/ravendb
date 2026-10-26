import { useMemo, useState } from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import { SortDirection } from "components/models/common";
import { databaseLocationComparator } from "components/utils/common";
import IconName from "typings/server/icons";
import {
    CheckboxListFilterCategory,
    FilterCategory,
    isFilterCategoryActive,
    SingleChoiceFilterCategory,
} from "components/common/filterMenu/FilterMenu";
import { getReplicationProgressState, ReplicationProgressState } from "./ExternalReplicationTaskDistribution";
import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

export type SinkSortField = "databaseName" | "url" | "status" | "node";

export interface SinkFilters {
    databaseName: string;
    url: string;
    states: ReplicationProgressState[];
    statuses: OngoingTaskConnectionStatus[];
    hasErrors: boolean | null;
    nodeTags: string[];
}

export interface SinkSort {
    field: SinkSortField;
    direction: SortDirection;
}

export interface SinkSortFieldInfo {
    field: SinkSortField;
    label: string;
    icon: IconName;
}

export const sinkSortFields: SinkSortFieldInfo[] = [
    { field: "databaseName", label: "Sink database name", icon: "database" },
    { field: "url", label: "Actual sink URL", icon: "server" },
    { field: "status", label: "Replication status", icon: "connected" },
    { field: "node", label: "Node processing the sink", icon: "node" },
];

const defaultSinkSort: SinkSort = { field: "status", direction: "asc" };

export const sinkReplicationStateLabels: Record<ReplicationProgressState, string> = {
    UpToDate: "Up to date",
    Running: "Running",
    Disabled: "Disabled",
    Unknown: "Unknown",
};

export const sinkConnectionStatusLabels: Record<OngoingTaskConnectionStatus, string> = {
    None: "None",
    Active: "Active",
    NotActive: "Not active",
    Reconnect: "Reconnect",
    NotOnThisNode: "Not on this node",
};

const emptyFilters: SinkFilters = {
    databaseName: "",
    url: "",
    states: [],
    statuses: [],
    hasErrors: null,
    nodeTags: [],
};

export function getSinkKey(sink: OngoingTaskReplicationHubInfo): string {
    return `${sink.shared.destinationUrl ?? ""}/${sink.shared.destinationDatabase}`;
}

export function getVisibleNodes(sink: OngoingTaskReplicationHubInfo) {
    return sink.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && sink.responsibleLocations.some((l) => databaseLocationComparator(l, nodeInfo.location))
    );
}

function toFilterOptions<T extends string>(labels: Record<T, string>) {
    return Object.entries<string>(labels).map(([value, label]) => ({ value: value as T, label }));
}

function getSinkFilterCategories(
    filters: SinkFilters,
    updateFilters: (partialFilters: Partial<SinkFilters>) => void,
    nodeTags: string[]
): FilterCategory[] {
    return [
        {
            key: "databaseName",
            label: "Sink database name",
            icon: "database",
            type: "text",
            value: filters.databaseName,
            placeholder: "e.g. name",
            onChange: (databaseName) => updateFilters({ databaseName }),
        },
        {
            key: "state",
            label: "Replication state",
            icon: "changes",
            type: "checkboxList",
            options: toFilterOptions(sinkReplicationStateLabels),
            selectedValues: filters.states,
            onChange: (states) => updateFilters({ states }),
        } satisfies CheckboxListFilterCategory<ReplicationProgressState>,
        {
            key: "status",
            label: "Replication status",
            icon: "connected",
            type: "checkboxList",
            options: toFilterOptions(sinkConnectionStatusLabels),
            selectedValues: filters.statuses,
            onChange: (statuses) => updateFilters({ statuses }),
        } satisfies CheckboxListFilterCategory<OngoingTaskConnectionStatus>,
        {
            key: "hasErrors",
            label: "Has errors",
            icon: "warning",
            type: "singleChoice",
            options: [
                { value: true, label: "Yes" },
                { value: false, label: "No" },
            ],
            selectedValue: filters.hasErrors,
            onChange: (hasErrors) => updateFilters({ hasErrors }),
        } satisfies SingleChoiceFilterCategory<boolean>,
        {
            key: "url",
            label: "Actual sink URL",
            icon: "server",
            type: "text",
            value: filters.url,
            placeholder: "e.g. https://",
            onChange: (url) => updateFilters({ url }),
        },
        {
            key: "node",
            label: "Node processing the sink",
            icon: "node",
            type: "checkboxList",
            options: nodeTags.map((x) => ({ value: x, label: `Node ${x}` })),
            selectedValues: filters.nodeTags,
            onChange: (selectedNodeTags) => updateFilters({ nodeTags: selectedNodeTags }),
        },
    ];
}

function matchesFilters(sink: OngoingTaskReplicationHubInfo, filters: SinkFilters): boolean {
    const visibleNodes = getVisibleNodes(sink);

    const databaseNameTerm = filters.databaseName.trim().toLowerCase();
    if (databaseNameTerm && !sink.shared.destinationDatabase?.toLowerCase().includes(databaseNameTerm)) {
        return false;
    }

    const urlTerm = filters.url.trim().toLowerCase();
    if (urlTerm && !sink.shared.destinationUrl?.toLowerCase().includes(urlTerm)) {
        return false;
    }

    if (
        filters.states.length > 0 &&
        !visibleNodes.some((x) => filters.states.includes(getReplicationProgressState(sink, x)))
    ) {
        return false;
    }

    if (
        filters.statuses.length > 0 &&
        !visibleNodes.some((x) => filters.statuses.includes(x.details.taskConnectionStatus))
    ) {
        return false;
    }

    if (filters.hasErrors != null && visibleNodes.some((x) => !!x.details.error) !== filters.hasErrors) {
        return false;
    }

    if (filters.nodeTags.length > 0 && !visibleNodes.some((x) => filters.nodeTags.includes(x.location.nodeTag))) {
        return false;
    }

    return true;
}

const sinkWithErrorRank = 0;

const connectionStatusRanks: Record<OngoingTaskConnectionStatus, number> = {
    Reconnect: 1,
    NotActive: 2,
    None: 3,
    NotOnThisNode: 4,
    Active: 5,
};

function getStatusRank(sink: OngoingTaskReplicationHubInfo): number {
    const nodeRanks = getVisibleNodes(sink).map((x) =>
        x.details.error ? sinkWithErrorRank : connectionStatusRanks[x.details.taskConnectionStatus]
    );

    return nodeRanks.length > 0 ? Math.min(...nodeRanks) : connectionStatusRanks.None;
}

export function getSinkConnectionStatus(sink: OngoingTaskReplicationHubInfo): OngoingTaskConnectionStatus | null {
    const statuses = getVisibleNodes(sink).map((x) => x.details.taskConnectionStatus);

    if (statuses.length === 0) {
        return null;
    }

    return statuses.reduce((worst, x) => (connectionStatusRanks[x] < connectionStatusRanks[worst] ? x : worst));
}

export function getSinkError(sink: OngoingTaskReplicationHubInfo): string | null {
    return getVisibleNodes(sink).find((x) => x.details.error)?.details.error ?? null;
}

function compareText(a: string, b: string): number {
    return (a ?? "").localeCompare(b ?? "", undefined, { numeric: true });
}

function compareByField(a: OngoingTaskReplicationHubInfo, b: OngoingTaskReplicationHubInfo, field: SinkSortField) {
    switch (field) {
        case "databaseName":
            return compareText(a.shared.destinationDatabase, b.shared.destinationDatabase);
        case "url":
            return compareText(a.shared.destinationUrl, b.shared.destinationUrl);
        case "status":
            return getStatusRank(a) - getStatusRank(b);
        case "node":
            return compareText(getVisibleNodes(a)[0]?.location.nodeTag, getVisibleNodes(b)[0]?.location.nodeTag);
    }
}

export function useReplicationHubSinks(connectedSinks: OngoingTaskReplicationHubInfo[]) {
    const [filters, setFilters] = useState<SinkFilters>(emptyFilters);
    const [sort, setSort] = useState<SinkSort>(defaultSinkSort);
    const [expandedSinkKeys, setExpandedSinkKeys] = useState<string[]>([]);

    const nodeTags = useMemo(
        () =>
            Array.from(
                new Set(connectedSinks.flatMap((sink) => getVisibleNodes(sink).map((x) => x.location.nodeTag)))
            ).sort(),
        [connectedSinks]
    );

    const visibleSinks = useMemo(() => {
        const filtered = connectedSinks.filter((sink) => matchesFilters(sink, filters));
        const directionMultiplier = sort.direction === "asc" ? 1 : -1;

        return [...filtered].sort(
            (a, b) =>
                compareByField(a, b, sort.field) * directionMultiplier ||
                compareText(a.shared.destinationDatabase, b.shared.destinationDatabase)
        );
    }, [connectedSinks, filters, sort]);

    const isDefaultSort = sort.field === defaultSinkSort.field && sort.direction === defaultSinkSort.direction;

    const resetSort = () => {
        setSort(defaultSinkSort);
    };

    const updateFilters = (partialFilters: Partial<SinkFilters>) => {
        setFilters((current) => ({ ...current, ...partialFilters }));
    };

    const filterCategories = getSinkFilterCategories(filters, updateFilters, nodeTags);
    const hasActiveFilters = filterCategories.some(isFilterCategoryActive);
    const totalSinksCount = connectedSinks.length;

    const areAllVisibleSinksExpanded =
        visibleSinks.length > 0 && visibleSinks.every((sink) => expandedSinkKeys.includes(getSinkKey(sink)));

    const toggleAllExpanded = () => {
        setExpandedSinkKeys(areAllVisibleSinksExpanded ? [] : visibleSinks.map(getSinkKey));
    };

    const toggleSinkExpanded = (sinkKey: string) => {
        setExpandedSinkKeys((keys) =>
            keys.includes(sinkKey) ? keys.filter((x) => x !== sinkKey) : [...keys, sinkKey]
        );
    };

    return {
        filterCategories,
        hasActiveFilters,
        totalSinksCount,
        sort,
        setSort,
        isDefaultSort,
        resetSort,
        visibleSinks,
        expandedSinkKeys,
        toggleSinkExpanded,
        areAllVisibleSinksExpanded,
        toggleAllExpanded,
    };
}

export type ReplicationHubSinksState = ReturnType<typeof useReplicationHubSinks>;
