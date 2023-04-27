import { ComponentMeta } from "@storybook/react";
import React from "react";
import { StatePill } from "./StatePill";
import { boundCopy } from "../utils/common";
import { Spinner } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Icon } from "./Icon";

export default {
    title: "Bits/State Pills",
    decorators: [withStorybookContexts, withBootstrap5],
    component: StatePill,
} as ComponentMeta<typeof StatePill>;

const Template = () => {
    return (
        <div>
            <h1>State Pills and Badges</h1>

            <hr />
            <h3>Brand</h3>

            <div className="hstack gap-2">
                <StatePill>
                    <Icon icon="coffee" /> Neutral
                </StatePill>
                <StatePill color="primary">
                    <Icon icon="raven" /> Primary
                </StatePill>
            </div>

            <hr />
            <h3>Function</h3>

            <div className="hstack gap-2">
                <StatePill color="secondary">
                    <Icon icon="zombie" /> Secondary
                </StatePill>
                <StatePill color="success">
                    <Icon icon="check" /> Success!
                </StatePill>
                <StatePill color="warning">
                    <Icon icon="warning" /> Warning
                </StatePill>
                <StatePill color="danger">
                    <Icon icon="danger" /> Danger
                </StatePill>
                <StatePill color="info">
                    <Icon icon="help" /> Info
                </StatePill>
            </div>

            <hr />
            <h3>Studio</h3>

            <div className="hstack gap-2">
                <StatePill color="shard">
                    <Icon icon="shard" /> Shard
                </StatePill>
                <StatePill color="node">
                    <Icon icon="node" /> Node
                </StatePill>
                <StatePill color="orchestrator">
                    <Icon icon="orchestrator" /> Orchestrator
                </StatePill>
                <StatePill color="progress">
                    <Spinner size="xs" className="me-1" /> Progress
                </StatePill>
            </div>
        </div>
    );
};

export const Pills = boundCopy(Template);
