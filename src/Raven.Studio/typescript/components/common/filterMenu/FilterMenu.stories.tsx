import { Meta, StoryObj } from "@storybook/react-webpack5";
import { useState } from "react";
import moment from "moment";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import {
    CheckboxListFilterCategory,
    DateRange,
    FilterCategory,
    FilterChips,
    FilterMenu,
    SingleChoiceFilterCategory,
} from "./FilterMenu";

export default {
    title: "Bits/FilterMenu",
    decorators: [withStorybookContexts, withBootstrap5],
    render: (args) => <FilterMenuStory {...args} />,
} satisfies Meta<FilterMenuStoryArgs>;

type Region = "Europe" | "America" | "Asia";

interface Server {
    name: string;
    region: Region;
    isOnline: boolean;
    lastSeen: Date;
}

const daysAgo = (days: number) => moment().subtract(days, "days").toDate();

const servers: Server[] = [
    { name: "orders-eu-1", region: "Europe", isOnline: true, lastSeen: daysAgo(0) },
    { name: "orders-eu-2", region: "Europe", isOnline: false, lastSeen: daysAgo(3) },
    { name: "billing-us-1", region: "America", isOnline: true, lastSeen: daysAgo(1) },
    { name: "billing-us-2", region: "America", isOnline: true, lastSeen: daysAgo(7) },
    { name: "search-asia-1", region: "Asia", isOnline: false, lastSeen: daysAgo(14) },
    { name: "search-asia-2", region: "Asia", isOnline: true, lastSeen: daysAgo(2) },
];

const regionOptions: { value: Region; label: string }[] = [
    { value: "Europe", label: "Europe" },
    { value: "America", label: "America" },
    { value: "Asia", label: "Asia" },
];

interface FilterMenuStoryArgs {
    label?: string;
}

function FilterMenuStory({ label }: FilterMenuStoryArgs) {
    const [name, setName] = useState("");
    const [regions, setRegions] = useState<Region[]>([]);
    const [isOnline, setIsOnline] = useState<boolean>(null);
    const [lastSeen, setLastSeen] = useState<DateRange>({ start: null, end: null });

    const categories: FilterCategory[] = [
        {
            key: "name",
            label: "Server name",
            icon: "server",
            type: "text",
            value: name,
            placeholder: "e.g. orders",
            onChange: setName,
        },
        {
            key: "region",
            label: "Region",
            icon: "global",
            type: "checkboxList",
            options: regionOptions,
            selectedValues: regions,
            onChange: (values) => setRegions(values),
        } satisfies CheckboxListFilterCategory<Region>,
        {
            key: "online",
            label: "Online",
            icon: "connected",
            type: "singleChoice",
            options: [
                { value: true, label: "Yes" },
                { value: false, label: "No" },
            ],
            selectedValue: isOnline,
            onChange: setIsOnline,
        } satisfies SingleChoiceFilterCategory<boolean>,
        {
            key: "lastSeen",
            label: "Last seen",
            icon: "calendar",
            type: "dateRange",
            value: lastSeen,
            onChange: setLastSeen,
        },
    ];

    const visibleServers = servers.filter(
        (x) =>
            x.name.includes(name.trim()) &&
            (regions.length === 0 || regions.includes(x.region)) &&
            (isOnline == null || x.isOnline === isOnline) &&
            (lastSeen.start == null || x.lastSeen >= lastSeen.start) &&
            (lastSeen.end == null || x.lastSeen <= lastSeen.end)
    );

    return (
        <div className="vstack gap-2" style={{ maxWidth: 600 }}>
            <div className="d-flex justify-content-between align-items-center">
                <h4 className="mb-0">Servers</h4>
                <ButtonGroup size="sm">
                    <FilterMenu categories={categories} title="Filter servers" label={label} />
                </ButtonGroup>
            </div>
            <FilterChips categories={categories}>
                <span className="text-muted ms-1">
                    {visibleServers.length} of {servers.length} servers
                </span>
            </FilterChips>
            <ul className="mb-0">
                {visibleServers.map((x) => (
                    <li key={x.name}>
                        {x.name} ({x.region}, {x.isOnline ? "online" : "offline"})
                    </li>
                ))}
            </ul>
        </div>
    );
}

export const Default: StoryObj<FilterMenuStoryArgs> = {};

export const WithLabel: StoryObj<FilterMenuStoryArgs> = {
    args: { label: "Filter" },
};
