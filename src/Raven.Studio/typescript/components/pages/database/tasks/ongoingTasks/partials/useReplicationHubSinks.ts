import { useState } from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import { SortDirection } from "components/models/common";
import IconName from "typings/server/icons";
import {
    CheckboxListFilterCategory,
    FilterCategory,
    isFilterCategoryActive,
} from "components/common/filterMenu/FilterMenu";
import {
    getReplicationProgressState,
    getVisibleNodes,
    ReplicationProgressState,
} from "./ExternalReplicationTaskDistribution";

export type SinkSortField = "databaseName" | "url" | "node";

interface SinkFilters {
    databaseName: string;
    url: string;
    states: ReplicationProgressState[];
    nodeTags: string[];
}

interface SinkSort {
    field: SinkSortField;
    direction: SortDirection;
}

interface SinkSortFieldInfo {
    field: SinkSortField;
    label: string;
    icon: IconName;
}

export const sinkSortFields: SinkSortFieldInfo[] = [
    { field: "databaseName", label: "Sink database name", icon: "database" },
    { field: "url", label: "Actual sink URL", icon: "server" },
    { field: "node", label: "Node processing the sink", icon: "node" },
];

const defaultSinkSort: SinkSort = { field: "databaseName", direction: "asc" };

const sinkReplicationStateLabels: Record<ReplicationProgressState, string> = {
    UpToDate: "Up to date",
    Running: "Running",
    Disabled: "Disabled",
    Unknown: "Unknown",
};

const emptyFilters: SinkFilters = {
    databaseName: "",
    url: "",
    states: [],
    nodeTags: [],
};

export function getSinkKey(sink: OngoingTaskReplicationHubInfo): string {
    return `${sink.shared.destinationUrl ?? ""}/${sink.shared.destinationDatabase}`;
}

function toFilterOptions<T extends string>(labels: Record<T, string>) {
    return Object.entries<string>(labels).map(([value, label]) => ({ value: value as T, label }));
}

function getSinkFilterCategories(
    filters: SinkFilters,
    updateFilters: (partialFilters: Partial<SinkFilters>) => void,
    nodeTags: string[]
): FilterCategory[] {
    const nodeTagOptions = Array.from(new Set([...nodeTags, ...filters.nodeTags])).sort();

    return [
        {
            key: "databaseName",
            label: "Sink database name",
            icon: "database",
            type: "text",
            value: filters.databaseName,
            placeholder: "e.g. Orders",
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
            key: "url",
            label: "Actual sink URL",
            icon: "server",
            type: "text",
            value: filters.url,
            placeholder: "e.g. sink.example.com",
            onChange: (url) => updateFilters({ url }),
        },
        {
            key: "node",
            label: "Node processing the sink",
            icon: "node",
            type: "checkboxList",
            options: nodeTagOptions.map((x) => ({ value: x, label: `Node ${x}` })),
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

    if (filters.nodeTags.length > 0 && !visibleNodes.some((x) => filters.nodeTags.includes(x.location.nodeTag))) {
        return false;
    }

    return true;
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
        case "node":
            return compareText(getVisibleNodes(a)[0]?.location.nodeTag, getVisibleNodes(b)[0]?.location.nodeTag);
    }
}

export function useReplicationHubSinks(connectedSinks: OngoingTaskReplicationHubInfo[]) {
    const [filters, setFilters] = useState<SinkFilters>(emptyFilters);
    const [sort, setSort] = useState<SinkSort>(defaultSinkSort);
    const [expandedSinkKeys, setExpandedSinkKeys] = useState<string[]>([]);

    const nodeTags = Array.from(
        new Set(connectedSinks.flatMap((sink) => getVisibleNodes(sink).map((x) => x.location.nodeTag)))
    ).sort();

    const directionMultiplier = sort.direction === "asc" ? 1 : -1;
    const visibleSinks = connectedSinks
        .filter((sink) => matchesFilters(sink, filters))
        .sort(
            (a, b) =>
                compareByField(a, b, sort.field) * directionMultiplier ||
                compareText(a.shared.destinationDatabase, b.shared.destinationDatabase)
        );

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
