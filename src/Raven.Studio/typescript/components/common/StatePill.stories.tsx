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
                    <Icon icon="coffee" className="me-1" /> Neutral
                </StatePill>
                <StatePill color="primary">
                    <Icon icon="raven" className="me-1" /> Primary
                </StatePill>
            </div>

            <hr />
            <h3>Function</h3>

            <div className="hstack gap-2">
                <StatePill color="secondary">
                    <Icon icon="zombie" className="me-1" /> Secondary
                </StatePill>
                <StatePill color="success">
                    <Icon icon="check" className="me-1" /> Success!
                </StatePill>
                <StatePill color="warning">
                    <Icon icon="warning" className="me-1" /> Warning
                </StatePill>
                <StatePill color="danger">
                    <Icon icon="danger" className="me-1" /> Danger
                </StatePill>
                <StatePill color="info">
                    <Icon icon="help" className="me-1" /> Info
                </StatePill>
            </div>

            <hr />
            <h3>Studio</h3>

            <div className="hstack gap-2">
                <StatePill color="shard">
                    <Icon icon="shard" className="me-1" /> Shard
                </StatePill>
                <StatePill color="node">
                    <Icon icon="node" className="me-1" /> Node
                </StatePill>
                <StatePill color="orchestrator">
                    <Icon icon="orchestrator" className="me-1" /> Orchestrator
                </StatePill>
                <StatePill color="progress">
                    <Spinner size="xs" /> Progress
                </StatePill>
            </div>
        </div>
    );
};

export const Pills = boundCopy(Template);
