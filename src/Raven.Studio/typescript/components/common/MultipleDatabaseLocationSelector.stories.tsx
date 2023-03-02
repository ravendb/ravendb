import { ComponentMeta } from "@storybook/react";
import React, { useState } from "react";
import { MultipleDatabaseLocationSelector } from "./MultipleDatabaseLocationSelector";
import { boundCopy } from "../utils/common";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Database location selector - multiple",
    component: MultipleDatabaseLocationSelector,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof MultipleDatabaseLocationSelector>;

const Template = (args: { locations: databaseLocationSpecifier[] }) => {
    const [locations, setLocations] = useState<databaseLocationSpecifier[]>([]);

    return (
        <MultipleDatabaseLocationSelector
            locations={args.locations}
            selectedLocations={locations}
            setSelectedLocations={setLocations}
        />
    );
};

export const ShardedDatabase = boundCopy(Template);
ShardedDatabase.args = {
    locations: [
        {
            nodeTag: "A",
            shardNumber: 0,
        },
        {
            nodeTag: "A",
            shardNumber: 1,
        },
        {
            nodeTag: "B",
            shardNumber: 0,
        },
        {
            nodeTag: "B",
            shardNumber: 1,
        },
        {
            nodeTag: "B",
            shardNumber: 2,
        },
        {
            nodeTag: "C",
            shardNumber: 0,
        },
        {
            nodeTag: "C",
            shardNumber: 2,
        },
    ],
};

export const NonShardedDatabase = boundCopy(Template);
NonShardedDatabase.args = {
    locations: [
        {
            nodeTag: "A",
        },
        {
            nodeTag: "B",
        },
        {
            nodeTag: "C",
        },
    ],
};
