import { ComponentMeta } from "@storybook/react";
import React from "react";
import { DatabaseLocationSelector } from "./DatabaseLocationSelector";

export default {
    title: "Database location selector",
    component: DatabaseLocationSelector
} as ComponentMeta<typeof DatabaseLocationSelector>;

const Template = (args: { locations: databaseLocationSpecifier[] }) => {
    return (
        <DatabaseLocationSelector locations={args.locations} selectedLocations={[]} setSelectedLocations={() => {
        }}/>
    )
}

export const ShardedDatabase = Template.bind({});
ShardedDatabase.args = {
    locations: [{
        nodeTag: "A",
        shardNumber: 0
    },
        {
            nodeTag: "B",
            shardNumber: 1
        }]
}

export const NonShardedDatabase = Template.bind({});
NonShardedDatabase.args = {
    locations: [{
        nodeTag: "A"
    }, {
        nodeTag: "B"
    }, {
        nodeTag: "C"
    }]
}
