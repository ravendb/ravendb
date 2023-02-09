import { ComponentMeta } from "@storybook/react";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "./PropSummary";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Card } from "reactstrap";
import { Icon } from "./Icon";

export default {
    title: "Bits/PropSummary",
    component: PropSummary,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof PropSummary>;

export function PropertySummary() {
    return (
        <Card className="p-4">
            <PropSummary>
                <PropSummaryItem>
                    <PropSummaryName>
                        <Icon icon="encryption" className="me-1" /> Encryption
                    </PropSummaryName>
                    <PropSummaryValue color="danger">OFF</PropSummaryValue>
                </PropSummaryItem>
                <PropSummaryItem>
                    <PropSummaryName>
                        <Icon icon="replication" className="me-1" /> Replication
                    </PropSummaryName>
                    <PropSummaryValue color="danger">OFF</PropSummaryValue>
                </PropSummaryItem>
                <PropSummaryItem>
                    <PropSummaryName>
                        <Icon icon="sharding" className="me-1" /> Sharding
                    </PropSummaryName>
                    <PropSummaryValue color="danger">OFF</PropSummaryValue>
                </PropSummaryItem>
                <PropSummaryItem>
                    <PropSummaryName>
                        <Icon icon="path" className="me-1" /> <strong>Default</strong> paths
                    </PropSummaryName>
                </PropSummaryItem>
            </PropSummary>
        </Card>
    );
}
