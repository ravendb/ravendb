import { ComponentMeta } from "@storybook/react";
import React, { useState } from "react";
import { SingleDatabaseLocationSelector } from "./SingleDatabaseLocationSelector";

export default {
    title: "Bits/Database location selector - single",
    component: SingleDatabaseLocationSelector,
} as ComponentMeta<typeof SingleDatabaseLocationSelector>;

const Template = (args: { locations: databaseLocationSpecifier[] }) => {
    const [location, setLocation] = useState<databaseLocationSpecifier>();

    return (
        <SingleDatabaseLocationSelector
            locations={args.locations}
            selectedLocation={location}
            setSelectedLocation={setLocation}
        />
    );
};

export const ShardedDatabase = Template.bind({});
ShardedDatabase.args = {
    locations: [
        {
            nodeTag: "A",
            shardNumber: 0,
        },
        {
            nodeTag: "B",
            shardNumber: 1,
        },
    ],
};

export const NonShardedDatabase = Template.bind({});
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
