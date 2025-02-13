import { Meta } from "@storybook/react";
import React, { useState } from "react";
import { DatabaseActionContexts, MultipleDatabaseLocationSelector } from "./MultipleDatabaseLocationSelector";
import { boundCopy } from "../utils/common";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Database location selector - multiple",
    component: MultipleDatabaseLocationSelector,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-10520&t=ipd4AuT8v0is7yTp-4",
        },
    },
} satisfies Meta<typeof MultipleDatabaseLocationSelector>;

const Template = (args: { allContexts: DatabaseActionContexts[] }) => {
    const [selectedContexts, setSelectedContexts] = useState<DatabaseActionContexts[]>(args.allContexts);

    return (
        <MultipleDatabaseLocationSelector
            allActionContexts={args.allContexts}
            selectedActionContexts={selectedContexts}
            setSelectedActionContexts={setSelectedContexts}
        />
    );
};

export const ShardedDatabase = boundCopy(Template);
ShardedDatabase.args = {
    allContexts: [
        {
            nodeTag: "A",
            shardNumbers: [0, 1],
        },
        {
            nodeTag: "B",
            shardNumbers: [0, 1, 2],
        },
        {
            nodeTag: "C",
            shardNumbers: [0, 2],
        },
    ],
};

export const ShardedDatabaseWithOrchestrator = boundCopy(Template);
ShardedDatabaseWithOrchestrator.args = {
    allContexts: [
        {
            nodeTag: "A",
            shardNumbers: [0, 1],
            includeOrchestrator: true,
        },
        {
            nodeTag: "B",
            shardNumbers: [0, 1, 2],
        },
        {
            nodeTag: "C",
            shardNumbers: [0, 2],
        },
    ],
};

export const NonShardedDatabase = boundCopy(Template);
NonShardedDatabase.args = {
    allContexts: [
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
