import { Meta } from "@storybook/react";
import { Checkbox, Radio, Switch } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Check",
    decorators: [withStorybookContexts, withBootstrap5],
    args: {
        isReversed: false,
        isDisabled: false,
        hasLabel: true,
    },
} satisfies Meta;

interface AllChecksVariantsProps {
    size?: "sm" | "lg";
    active?: boolean;
    disabled?: boolean;
    reversed?: boolean;
    hasLabel?: boolean;
}

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

export function CheckboxVariants({
    isReversed,
    isDisabled,
    hasLabel,
}: {
    isReversed: boolean;
    isDisabled: boolean;
    hasLabel: boolean;
}) {
    return (
        <>
            <div className="mt-4">
                <h3>Default size</h3>
                <AllCheckboxVariants reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size lg</h3>
                <AllCheckboxVariants size="lg" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size sm</h3>
                <AllCheckboxVariants size="sm" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
        </>
    );
}

export function RadioVariants({
    isReversed,
    isDisabled,
    hasLabel,
}: {
    isReversed: boolean;
    isDisabled: boolean;
    hasLabel: boolean;
}) {
    return (
        <>
            <div className="mt-4">
                <h3>Default size</h3>
                <AllRadioVariants reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size lg</h3>
                <AllRadioVariants size="lg" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size sm</h3>
                <AllRadioVariants size="sm" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
        </>
    );
}

export function SwitchVariants({
    isReversed,
    isDisabled,
    hasLabel,
}: {
    isReversed: boolean;
    isDisabled: boolean;
    hasLabel: boolean;
}) {
    return (
        <>
            <div className="mt-4">
                <h3>Default size</h3>
                <ALlSwitchVariants reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size lg</h3>
                <ALlSwitchVariants size="lg" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
            <hr />
            <div className="mt-4">
                <h3>Size sm</h3>
                <ALlSwitchVariants size="sm" reversed={isReversed} disabled={isDisabled} hasLabel={hasLabel} />
            </div>
        </>
    );
}

function AllCheckboxVariants(props: AllChecksVariantsProps) {
    const { active, disabled, hasLabel, reversed, size } = props;
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div className="hstack gap-3">
            {colors.map((color) => (
                <Checkbox
                    color={color}
                    size={size}
                    selected={selected}
                    toggleSelection={toggle}
                    active={active}
                    disabled={disabled}
                    reverse={reversed}
                >
                    {hasLabel && color}
                </Checkbox>
            ))}
        </div>
    );
}

function AllRadioVariants(props: AllChecksVariantsProps) {
    const { active, disabled, hasLabel, reversed, size } = props;
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div className="hstack gap-3">
            {colors.map((color) => (
                <Radio
                    color={color}
                    size={size}
                    selected={selected}
                    toggleSelection={toggle}
                    active={active}
                    disabled={disabled}
                    reverse={reversed}
                >
                    {hasLabel && color}
                </Radio>
            ))}
        </div>
    );
}

function ALlSwitchVariants(props: AllChecksVariantsProps) {
    const { active, disabled, hasLabel, reversed, size } = props;
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div className="hstack gap-3">
            {colors.map((color) => (
                <Switch
                    color={color}
                    size={size}
                    selected={selected}
                    toggleSelection={toggle}
                    active={active}
                    disabled={disabled}
                    reverse={reversed}
                >
                    {hasLabel && color}
                </Switch>
            ))}
        </div>
    );
}
