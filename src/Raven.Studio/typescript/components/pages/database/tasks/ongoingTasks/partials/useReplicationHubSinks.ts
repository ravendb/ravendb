import { useMemo, useState } from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import { SortDirection } from "components/models/common";
import { databaseLocationComparator } from "components/utils/common";
import IconName from "typings/server/icons";
import { getReplicationProgressState, ReplicationProgressState } from "./ExternalReplicationTaskDistribution";
import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

export type SinkFilterCategory = "databaseName" | "state" | "status" | "hasErrors" | "url" | "node";

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

export interface SinkFilterCategoryInfo {
    category: SinkFilterCategory;
    label: string;
    icon: IconName;
}

export const sinkFilterCategories: SinkFilterCategoryInfo[] = [
    { category: "databaseName", label: "Sink database name", icon: "database" },
    { category: "state", label: "Replication state", icon: "changes" },
    { category: "status", label: "Replication status", icon: "connected" },
    { category: "hasErrors", label: "Has errors", icon: "warning" },
    { category: "url", label: "Actual sink URL", icon: "server" },
    { category: "node", label: "Node processing the sink", icon: "node" },
];

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

const defaultSinkSort: SinkSort = { field: "databaseName", direction: "asc" };

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

const filterKeysByCategory: Record<SinkFilterCategory, keyof SinkFilters> = {
    databaseName: "databaseName",
    url: "url",
    state: "states",
    status: "statuses",
    hasErrors: "hasErrors",
    node: "nodeTags",
};

export function getSinkKey(sink: OngoingTaskReplicationHubInfo): string {
    return `${sink.shared.destinationUrl ?? ""}/${sink.shared.destinationDatabase}`;
}

function getVisibleNodes(sink: OngoingTaskReplicationHubInfo) {
    return sink.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && sink.responsibleLocations.some((l) => databaseLocationComparator(l, nodeInfo.location))
    );
}

export function isSinkFilterActive(filters: SinkFilters, category: SinkFilterCategory): boolean {
    switch (category) {
        case "databaseName":
            return filters.databaseName.trim() !== "";
        case "url":
            return filters.url.trim() !== "";
        case "state":
            return filters.states.length > 0;
        case "status":
            return filters.statuses.length > 0;
        case "hasErrors":
            return filters.hasErrors != null;
        case "node":
            return filters.nodeTags.length > 0;
    }
}

export function getSinkFilterValueLabel(filters: SinkFilters, category: SinkFilterCategory): string {
    switch (category) {
        case "databaseName":
            return filters.databaseName;
        case "url":
            return filters.url;
        case "state":
            return filters.states.map((x) => sinkReplicationStateLabels[x]).join(", ");
        case "status":
            return filters.statuses.map((x) => sinkConnectionStatusLabels[x]).join(", ");
        case "hasErrors":
            return filters.hasErrors ? "Yes" : "No";
        case "node":
            return filters.nodeTags.join(", ");
    }
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

    const activeFilterCategories = sinkFilterCategories.filter((x) => isSinkFilterActive(filters, x.category));
    const hasActiveFilters = activeFilterCategories.length > 0;
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

    const updateFilters = (partialFilters: Partial<SinkFilters>) => {
        setFilters((current) => ({ ...current, ...partialFilters }));
    };

    const clearFilters = () => {
        setFilters(emptyFilters);
    };

    const clearFilter = (category: SinkFilterCategory) => {
        const filterKey = filterKeysByCategory[category];
        setFilters((current) => ({ ...current, [filterKey]: emptyFilters[filterKey] }));
    };

    return {
        filters,
        updateFilters,
        clearFilters,
        clearFilter,
        activeFilterCategories,
        hasActiveFilters,
        totalSinksCount,
        sort,
        setSort,
        nodeTags,
        visibleSinks,
        expandedSinkKeys,
        toggleSinkExpanded,
        areAllVisibleSinksExpanded,
        toggleAllExpanded,
    };
}

export type ReplicationHubSinksState = ReturnType<typeof useReplicationHubSinks>;
