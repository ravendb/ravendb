import { ComponentMeta } from "@storybook/react";
import React from "react";
import { StatePill } from "./StatePill";
import { boundCopy } from "../utils/common";
import { Spinner } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

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
                    <i className="icon-coffee" /> Neutral
                </StatePill>
                <StatePill color="primary">
                    <i className="icon-raven" /> Primary
                </StatePill>
            </div>

            <hr />
            <h3>Function</h3>

            <div className="hstack gap-2">
                <StatePill color="secondary">
                    <i className="icon-zombie" /> Secondary
                </StatePill>
                <StatePill color="success">
                    <i className="icon-check" /> Success!
                </StatePill>
                <StatePill color="warning">
                    <i className="icon-warning" /> Warning
                </StatePill>
                <StatePill color="danger">
                    <i className="icon-danger" /> Danger
                </StatePill>
                <StatePill color="info">
                    <i className="icon-help" /> Info
                </StatePill>
            </div>

            <hr />
            <h3>Studio</h3>

            <div className="hstack gap-2">
                <StatePill color="shard">
                    <i className="icon-shard" /> Shard
                </StatePill>
                <StatePill color="node">
                    <i className="icon-node" /> Node
                </StatePill>
                <StatePill color="orchestrator">
                    <i className="icon-orchestrator" /> Orchestrator
                </StatePill>
                <StatePill color="progress">
                    <Spinner size="xs" /> Progress
                </StatePill>
            </div>
        </div>
    );
};

export const Pills = boundCopy(Template);
