import { Meta } from "@storybook/react";
import React, { useState } from "react";
import { SingleDatabaseLocationSelector } from "./SingleDatabaseLocationSelector";
import { boundCopy } from "../utils/common";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Database location selector - single",
    component: SingleDatabaseLocationSelector,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-10558&t=ipd4AuT8v0is7yTp-4",
        },
    },
} satisfies Meta<typeof SingleDatabaseLocationSelector>;

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
            shardNumber: 0,
        },
        {
            nodeTag: "A",
            shardNumber: 1,
        },
        {
            nodeTag: "C",
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
