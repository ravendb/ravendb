import { ComponentMeta } from "@storybook/react";
import React, { useState } from "react";
import { SingleDatabaseLocationSelector } from "./SingleDatabaseLocationSelector";
import { boundCopy } from "../utils/common";

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

export const ShardedDatabase = boundCopy(Template);
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
