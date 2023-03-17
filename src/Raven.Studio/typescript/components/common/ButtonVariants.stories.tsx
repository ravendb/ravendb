import { ComponentMeta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button, Card, Col, Row } from "reactstrap";
export default {
    title: "Bits/Buttons",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Button,
} as ComponentMeta<typeof Button>;

export function Variants() {
    return (
        <>
            <Card className="p-4">
                <AllButtons />
            </Card>
        </>
    );
}

function AllButtons() {
    return (
        <>
            <div className="mt-4">
                <h3>Default</h3>
                <AllButtonColors />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Default size, active</h3>
                <AllButtonColors active />
            </div>
            <div className="mt-4">
                <h3>Default size, disabled</h3>
                <AllButtonColors disabled />
            </div>
            <div className="mt-4">
                <h3>Default size, outline</h3>
                <AllButtonColors outline />
            </div>

            <div className="mt-4">
                <h3>Size lg</h3>
                <AllButtonColors size="lg" />
            </div>
            <div className="mt-4">
                <h3>Default size</h3>
                <AllButtonColors />
            </div>
            <div className="mt-4">
                <h3>Size sm</h3>
                <AllButtonColors size="sm" />
            </div>
            <div className="mt-4">
                <h3>Size xs</h3>
                <AllButtonColors size="xs" />
            </div>
        </>
    );
}

interface AllButtonColorsProps {
    size?: string;
    outline?: boolean;
    active?: boolean;
    disabled?: boolean;
}

function AllButtonColors(props: AllButtonColorsProps) {
    const { size, outline, active, disabled } = props;

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
        "dark",
        "light",
    ];

    return (
        <div className="hstack gap-1">
            {colors.map((color) => (
                <Button color={color} size={size} outline={outline} active={active} disabled={disabled}>
                    {color}
                </Button>
            ))}
        </div>
    );
}
