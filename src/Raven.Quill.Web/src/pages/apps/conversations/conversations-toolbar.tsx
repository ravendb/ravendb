import { Search } from "lucide-react";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/shadcn/ui/select";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { ConversationStateDot } from "@/pages/apps/conversations/conversation-state";

export interface StatusFilterOption {
    value: string;
    label: string;
    count: number;
}

export interface FilterOption {
    value: string;
    label: string;
}

interface ConversationsToolbarProps {
    search: string;
    onSearchChange: (value: string) => void;
    status: string;
    onStatusChange: (value: string) => void;
    statusOptions: StatusFilterOption[];
    totalCount: number;
    agent: string;
    onAgentChange: (value: string) => void;
    agentOptions: FilterOption[];
    channel: string;
    onChannelChange: (value: string) => void;
    channelOptions: FilterOption[];
}

export function ConversationsToolbar({
    search,
    onSearchChange,
    status,
    onStatusChange,
    statusOptions,
    totalCount,
    agent,
    onAgentChange,
    agentOptions,
    channel,
    onChannelChange,
    channelOptions,
}: ConversationsToolbarProps) {
    return (
        <div className="flex flex-wrap items-center gap-3">
            <InputGroup className="w-full sm:max-w-xs">
                <InputGroupAddon>
                    <Search />
                </InputGroupAddon>
                <InputGroupInput
                    placeholder="Search messages or sessions"
                    value={search}
                    onChange={(event) => onSearchChange(event.target.value)}
                />
            </InputGroup>

            <ToggleGroup
                type="single"
                variant="outline"
                size="sm"
                value={status}
                // radix clears the value when the active item is clicked again; keep "all" instead.
                onValueChange={(value) => onStatusChange(value || "all")}
            >
                <ToggleGroupItem value="all">
                    All
                    <FilterCount value={totalCount} />
                </ToggleGroupItem>
                {statusOptions.map((option) => (
                    <ToggleGroupItem key={option.value} value={option.value}>
                        <ConversationStateDot state={option.value} />
                        {option.label}
                        <FilterCount value={option.count} />
                    </ToggleGroupItem>
                ))}
            </ToggleGroup>

            <div className="flex items-center gap-2 sm:ml-auto">
                <FilterSelect
                    value={agent}
                    onChange={onAgentChange}
                    options={agentOptions}
                    allLabel="All agents"
                    ariaLabel="Filter by agent"
                />
                <FilterSelect
                    value={channel}
                    onChange={onChannelChange}
                    options={channelOptions}
                    allLabel="All channels"
                    ariaLabel="Filter by channel"
                />
            </div>
        </div>
    );
}

function FilterCount({ value }: { value: number }) {
    return <span className="text-muted-foreground tabular-nums">{value}</span>;
}

function FilterSelect({
    value,
    onChange,
    options,
    allLabel,
    ariaLabel,
}: {
    value: string;
    onChange: (value: string) => void;
    options: FilterOption[];
    allLabel: string;
    ariaLabel: string;
}) {
    return (
        <Select value={value} onValueChange={onChange}>
            <SelectTrigger size="sm" aria-label={ariaLabel} className="w-auto max-w-48 min-w-32">
                <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
                <SelectItem value="all">{allLabel}</SelectItem>
                {options.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                        {option.label}
                    </SelectItem>
                ))}
            </SelectContent>
        </Select>
    );
}
