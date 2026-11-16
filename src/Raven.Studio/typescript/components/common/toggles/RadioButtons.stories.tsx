import { useState } from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import RadioButtons from "./RadioButtons";

type AuthenticationMethod = "static" | "script";

interface RadioButtonsStoryArgs {
    selected: AuthenticationMethod;
    isDisabled: boolean;
    hasHelp: boolean;
    hasLongValues: boolean;
    containerWidth: number;
}

const LONG_LABEL = "External script that returns the destination settings of the production orders database";

function RadioButtonsStory({ selected, isDisabled, hasHelp, hasLongValues, containerWidth }: RadioButtonsStoryArgs) {
    const [selectedValue, setSelectedValue] = useState<AuthenticationMethod>(selected);

    return (
        <div style={{ maxWidth: containerWidth }}>
            <RadioButtons<AuthenticationMethod>
                aria-label="Authentication method"
                items={[
                    {
                        value: "static",
                        label: "Static credentials",
                        helpMessage: hasHelp ? "The credentials are saved with the task." : undefined,
                    },
                    {
                        value: "script",
                        label: hasLongValues ? LONG_LABEL : "External script",
                        helpMessage: hasHelp ? "A script returns the credentials on each run." : undefined,
                    },
                ]}
                selected={selectedValue}
                onSelect={setSelectedValue}
                isDisabled={isDisabled}
            />
        </div>
    );
}

export default {
    title: "Bits/RadioButtons",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    args: {
        selected: "static",
        isDisabled: false,
        hasHelp: false,
        hasLongValues: false,
        containerWidth: 640,
    },
    argTypes: {
        selected: { control: "radio", options: ["static", "script"] },
    },
    render: (args) => <RadioButtonsStory {...args} />,
} satisfies Meta<RadioButtonsStoryArgs>;

type Story = StoryObj<RadioButtonsStoryArgs>;

export const Default: Story = {};

export const WithHelp: Story = {
    args: { hasHelp: true },
};

export const Disabled: Story = {
    args: { isDisabled: true },
};

export const LongValues: Story = {
    args: { hasLongValues: true, hasHelp: true },
};

export const Narrow: Story = {
    args: { hasLongValues: true, hasHelp: true, containerWidth: 280 },
};
