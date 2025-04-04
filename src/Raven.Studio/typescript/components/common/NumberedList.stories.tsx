import { Meta, StoryObj } from "@storybook/react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";

export default {
    title: "Bits/NumberedList",
    decorators: [withStorybookContexts, withBootstrap5],
    component: NumberedList,
    argTypes: {
        length: { control: { type: "range", min: 2, max: 10, step: 1 } },
    },
} satisfies Meta;

interface NumberedListStoryProps {
    length: number;
}

export const Default: StoryObj<NumberedListStoryProps> = {
    args: {
        length: 3,
    },
    render: ({ length }) => {
        const items = Array.from({ length }, (_, i) => (
            <NumberedListItem key={i} stepKey={i}>
                This is a description for step {i + 1}
            </NumberedListItem>
        ));
        return <NumberedList>{items}</NumberedList>;
    },
};

export const CustomStepKey: StoryObj<NumberedListStoryProps> = {
    args: {
        length: 3,
    },
    render: ({ length }) => {
        const stepKeys = ["1️⃣", "2️⃣", "3️⃣", "✨", "🐦‍⬛", "💀", "😎", "🤢", "🤯", "💩"];
        const items = Array.from({ length }, (_, i) => (
            <NumberedListItem key={i} stepKey={stepKeys[i] || i + 1}>
                This is a description for step {stepKeys[i] || i + 1}
            </NumberedListItem>
        ));
        return <NumberedList>{items}</NumberedList>;
    },
};
