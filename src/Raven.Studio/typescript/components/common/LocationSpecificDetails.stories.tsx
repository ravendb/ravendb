import { Meta } from "@storybook/react";
import React from "react";
import { boundCopy } from "../utils/common";
import {
    LocationSpecificDetails,
    LocationSpecificDetailsItem,
    LocationSpecificDetailsItemsContainer,
} from "./LocationSpecificDetails";
import { StatePill } from "./StatePill";
import { NamedProgress, NamedProgressItem } from "./NamedProgress";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Icon } from "./Icon";

export default {
    title: "Bits/Location Specific Details",
    decorators: [withStorybookContexts, withBootstrap5],
    component: LocationSpecificDetails,
} satisfies Meta<typeof LocationSpecificDetails>;

const Template = (args: { withProgress: boolean; shardedLocation: boolean }) => {
    const location: databaseLocationSpecifier = {
        nodeTag: "A",
        shardNumber: args.shardedLocation ? 2 : undefined,
    };
    return (
        <div style={{ width: "500px" }}>
            <LocationSpecificDetails location={location}>
                <LocationSpecificDetailsItemsContainer>
                    <LocationSpecificDetailsItem>
                        <StatePill color="success">I&apos;m feeling good!</StatePill>
                    </LocationSpecificDetailsItem>
                    <LocationSpecificDetailsItem>
                        <Icon icon="list" /> 1,234,567 entries
                    </LocationSpecificDetailsItem>
                </LocationSpecificDetailsItemsContainer>

                {args.withProgress && (
                    <NamedProgress name="Progress #1">
                        <NamedProgressItem progress={{ processed: 0, total: 0 }}>subitem #1</NamedProgressItem>
                        <NamedProgressItem progress={{ processed: 75, total: 100 }}>subitem #2</NamedProgressItem>
                    </NamedProgress>
                )}

                {args.withProgress && (
                    <NamedProgress name="Progress #2">
                        <NamedProgressItem progress={{ processed: 100, total: 100 }}>subitem #1</NamedProgressItem>
                        <NamedProgressItem progress={{ processed: 0, total: 100 }}>subitem #2</NamedProgressItem>
                    </NamedProgress>
                )}

                {args.withProgress && (
                    <NamedProgress name="Progress #3">
                        <NamedProgressItem progress={{ processed: 100, total: 100 }}>subitem #1</NamedProgressItem>
                        <NamedProgressItem progress={{ processed: 75, total: 100 }}>subitem #2</NamedProgressItem>
                        <NamedProgressItem progress={{ processed: 25, total: 100 }}>
                            subitem with long name #3
                        </NamedProgressItem>
                    </NamedProgress>
                )}
            </LocationSpecificDetails>
        </div>
    );
};

export const ShardedWithProgress = boundCopy(Template);

ShardedWithProgress.args = {
    withProgress: true,
    shardedLocation: true,
};

export const ShardedWithOutProgress = boundCopy(Template);

ShardedWithOutProgress.args = {
    withProgress: false,
    shardedLocation: true,
};

export const NonShardedWithProgress = boundCopy(Template);

NonShardedWithProgress.args = {
    withProgress: true,
    shardedLocation: false,
};

export const NonShardedWithOutProgress = boundCopy(Template);

NonShardedWithOutProgress.args = {
    withProgress: false,
    shardedLocation: false,
};
