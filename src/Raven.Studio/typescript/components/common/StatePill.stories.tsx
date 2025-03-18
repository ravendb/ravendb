import { Meta } from "@storybook/react";
import React from "react";
import { StatePill } from "./StatePill";
import { boundCopy } from "../utils/common";
import Spinner from "react-bootstrap/Spinner";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Icon } from "./Icon";

export default {
    title: "Bits/State Pills",
    decorators: [withStorybookContexts, withBootstrap5],
    component: StatePill,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=9-50",
        },
    },
} satisfies Meta<typeof StatePill>;

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
                <StatePill bg="primary">
                    <Icon icon="raven" /> Primary
                </StatePill>
            </div>

            <hr />
            <h3>Function</h3>

            <div className="hstack gap-2">
                <StatePill bg="secondary">
                    <Icon icon="zombie" /> Secondary
                </StatePill>
                <StatePill bg="success">
                    <Icon icon="check" /> Success!
                </StatePill>
                <StatePill bg="warning">
                    <Icon icon="warning" /> Warning
                </StatePill>
                <StatePill bg="danger">
                    <Icon icon="danger" /> Danger
                </StatePill>
                <StatePill bg="info">
                    <Icon icon="help" /> Info
                </StatePill>
            </div>

            <hr />
            <h3>Studio</h3>

            <div className="hstack gap-2">
                <StatePill bg="shard">
                    <Icon icon="shard" /> Shard
                </StatePill>
                <StatePill bg="node">
                    <Icon icon="node" /> Node
                </StatePill>
                <StatePill bg="orchestrator">
                    <Icon icon="orchestrator" /> Orchestrator
                </StatePill>
                <StatePill bg="progress">
                    <Spinner size="xs" className="me-1" /> Progress
                </StatePill>
            </div>
        </div>
    );
};

export const Pills = boundCopy(Template);
