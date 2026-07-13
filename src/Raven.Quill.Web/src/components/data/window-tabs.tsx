import { Tabs, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";

// Time windows shared by the conversation/agent stats endpoints, which all return
// { last24h, last7d, last30d }. Mirrors the dashboard's "My apps" toggle labels.
export type WindowKey = "last24h" | "last7d" | "last30d";

const WINDOW_OPTIONS: { value: WindowKey; label: string }[] = [
    { value: "last24h", label: "Last 24 hours" },
    { value: "last7d", label: "Last 7 days" },
    { value: "last30d", label: "Last month" },
];

export function WindowTabs({ value, onChange }: { value: WindowKey; onChange: (value: WindowKey) => void }) {
    return (
        <Tabs value={value} onValueChange={(next) => onChange(next as WindowKey)}>
            <TabsList>
                {WINDOW_OPTIONS.map((option) => (
                    <TabsTrigger key={option.value} value={option.value}>
                        {option.label}
                    </TabsTrigger>
                ))}
            </TabsList>
        </Tabs>
    );
}
