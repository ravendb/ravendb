import { Meta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import ButtonWithSpinner from "./ButtonWithSpinner";
import Button from "react-bootstrap/Button";

export default {
    title: "Bits/Buttons",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Button,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=3-17352",
        },
    },
} satisfies Meta<typeof Button>;

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

export function Variants() {
    return <AllButtons />;
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
            <hr />
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
            <hr />
            <div className="mt-4">
                <h3>Button with spinner size lg</h3>
                <AllButtonWithSpinnerColors size="lg" />
            </div>
            <div className="mt-4">
                <h3>Button with spinner default size</h3>
                <AllButtonWithSpinnerColors />
            </div>
            <div className="mt-4">
                <h3>Button with spinner size sm</h3>
                <AllButtonWithSpinnerColors size="sm" />
            </div>
            <div className="mt-4">
                <h3>Button with spinner size xs</h3>
                <AllButtonWithSpinnerColors size="xs" />
            </div>
        </>
    );
}

interface AllButtonColorsProps {
    size?: "xs" | "sm" | "lg";
    outline?: boolean;
    active?: boolean;
    disabled?: boolean;
}

function AllButtonColors(props: AllButtonColorsProps) {
    const { size, outline, active, disabled } = props;

    return (
        <div className="hstack gap-1">
            {colors.map((color) => (
                <Button variant={outline ? `outline-${color}` : color} size={size} active={active} disabled={disabled}>
                    {color}
                </Button>
            ))}
        </div>
    );
}

function AllButtonWithSpinnerColors({ size }: AllButtonColorsProps) {
    return (
        <div className="hstack gap-1">
            {colors.map((color) => (
                <ButtonWithSpinner variant={color} size={size} isSpinning>
                    {color}
                </ButtonWithSpinner>
            ))}
        </div>
    );
}
