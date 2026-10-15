export type MapOrigin = { source: "planner"; planKey: string } | { source: "manual" } | { source: "import" };

type MapSource = MapOrigin["source"];

type MapSourceOption = { value: MapSource; label: string };

export const MAP_SOURCE_OPTIONS: MapSourceOption[] = [
    { value: "planner", label: "Planner" },
    { value: "manual", label: "Manual" },
    { value: "import", label: "Imported" },
];
