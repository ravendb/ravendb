import { Meta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Badge } from "reactstrap";
import IconName from "../../../typings/server/icons";
import { Icon } from "components/common/Icon";

export default {
    title: "Bits/Badges",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Badge,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=3-16074",
        },
    },
} satisfies Meta<typeof Badge>;

const colors = [
    "primary",
    "secondary",
    "success",
    "warning",
    "danger",
    "info",
    "progress",
    "node",
    "shard",
    "orchestrator",
    "dark",
    "light",
    "muted",
    "developer",
    "enterprise",
    "professional",
];

const fadedColors = [
    "faded-primary",
    "faded-secondary",
    "faded-success",
    "faded-warning",
    "faded-danger",
    "faded-info",
    "faded-progress",
    "faded-node",
    "faded-shard",
    "faded-orchestrator",
    "faded-dark",
    "faded-light",
    "faded-muted",
    "faded-developer",
    "faded-enterprise",
    "faded-professional",
];

interface AllBadgeColorsProps {
    icon?: IconName;
}

function AllBadgeColors(props: AllBadgeColorsProps) {
    const { icon } = props;
    return (
        <div className="hstack flex-wrap gap-1">
            {colors.map((color) => (
                <Badge color={color}>
                    {icon && <Icon icon={icon} />}
                    {color}
                </Badge>
            ))}
        </div>
    );
}

interface AllBadgeFadedColorsProps {
    icon?: IconName;
}

function AllBadgeFadedColors(props: AllBadgeFadedColorsProps) {
    const { icon } = props;
    return (
        <div className="hstack flex-wrap gap-1">
            {fadedColors.map((color) => (
                <Badge color={color}>
                    {icon && <Icon icon={icon} />}
                    {color}
                </Badge>
            ))}
        </div>
    );
}

function AllBadges() {
    return (
        <>
            <div className="mt-4">
                <h3>Default</h3>
                <AllBadgeColors />
            </div>
            <div className="mt-4">
                <h3>Default, with Icon</h3>
                <AllBadgeColors icon="zombie" />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Faded</h3>
                <AllBadgeFadedColors />
            </div>
            <div className="mt-4">
                <h3>Faded, with Icon</h3>
                <AllBadgeFadedColors icon="zombie" />
            </div>
        </>
    );
}

export function Variants() {
    return <AllBadges />;
}
