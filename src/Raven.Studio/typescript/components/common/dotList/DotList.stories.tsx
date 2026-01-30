import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { DotList } from "./DotList";
import { ThemeColor } from "components/models/common";
import { GapNumber } from "../utilities/stackCommon";

export default {
    title: "Bits/DotList",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface DotListStoryArgs {
    gap?: GapNumber;
    dotColor?: ThemeColor;
    lineColor?: ThemeColor;
}

export const Default: StoryObj<DotListStoryArgs> = {
    render: (args) => {
        return (
            <DotList
                gap={args.gap}
                dotColor={args.dotColor}
                lineColor={args.lineColor}
                items={[
                    <div>
                        First Item
                        <br />
                        Description
                    </div>,
                    <div>
                        Second Item
                        <br />
                        Description
                    </div>,
                    <div>
                        Third Item
                        <br />
                        Some long text
                        <br />
                        Some long text
                        <br />
                        Some long text
                    </div>,
                ]}
            />
        );
    },
    args: {
        gap: 2,
        dotColor: "primary",
        lineColor: "secondary",
    },
};
