import { ComponentMeta } from "@storybook/react";
import React from "react";
import { Spinner } from "reactstrap";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Spinners",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Spinner,
} as ComponentMeta<typeof Spinner>;

export function allSpinners() {
    return (
        <div>
            <h1>Spinners</h1>
            <div className="d-flex flex-column gap-3">
                <div className="d-flex flex-column">
                    <h3>Small</h3>
                    <AllSpinnerColors size="sm"></AllSpinnerColors>
                </div>
                <div className="d-flex flex-column">
                    <h3>Normal</h3>
                    <AllSpinnerColors></AllSpinnerColors>
                </div>
            </div>
            <h1 className="mt-3">Spinner w/ gradient</h1>
            <div className="d-flex flex-column gap-3">
                <div className="d-flex flex-column">
                    <h3>Small</h3>
                    <Spinner size="sm" className="spinner-gradient"></Spinner>
                </div>
                <div className="d-flex flex-column">
                    <h3>Normal</h3>
                    <Spinner className="spinner-gradient"></Spinner>
                </div>
            </div>
        </div>
    );
}

interface AllSpinnerColorsProps {
    size?: string;
}

function AllSpinnerColors(props: AllSpinnerColorsProps) {
    const { size } = props;
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
        <div className="hstack gap-3">
            {colors.map((color) => (
                <div className="d-flex flex-column align-items-center">
                    {color}
                    <Spinner color={color} size={size}></Spinner>
                </div>
            ))}
        </div>
    );
}
