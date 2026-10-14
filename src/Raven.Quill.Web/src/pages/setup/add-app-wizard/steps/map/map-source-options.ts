export type MapSource = "planner" | "manual";

/** `key` tells apart what produced the mapping: a plan, the manual scaffold, or an imported file. */
export type MapOrigin = { source: MapSource; key: string };

type MapSourceOption = { value: MapSource; label: string };

export const MAP_SOURCE_OPTIONS: MapSourceOption[] = [
    { value: "planner", label: "Planner" },
    { value: "manual", label: "Manual" },
];
