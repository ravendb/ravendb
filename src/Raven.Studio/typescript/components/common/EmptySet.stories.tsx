import { ComponentMeta } from "@storybook/react";
import { EmptySet } from "./EmptySet";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Icon } from "./Icon";
import { Badge } from "reactstrap";

export default {
    title: "Bits/EmptySet",
    component: EmptySet,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof EmptySet>;

export function EmptySets() {
    return (
        <div>
            <EmptySet>Use whenever a list is empty</EmptySet>
        </div>
    );
}

export function FeatureNotAvailable() {
    return (
        <div>
            <EmptySet icon="disabled" color="warning">
                <div className="vstack gap-3">
                    <span>
                        <Badge pill color="faded-warning">
                            Feature not available
                        </Badge>
                    </span>
                    <span>
                        Import documents from a CSV file into a collection is not available for{" "}
                        <Icon icon="sharding" color="shard" margin="m-0" /> sharded databases
                    </span>
                </div>
            </EmptySet>
        </div>
    );
}
