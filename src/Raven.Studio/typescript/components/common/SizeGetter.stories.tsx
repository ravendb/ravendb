import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import SizeGetter from "components/common/SizeGetter";

export default {
    title: "Bits/SizeGetter",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const SizeGetterStory: StoryObj = {
    name: "Size Getter",
    render: () => {
        return (
            <div style={{ height: "300px", width: "500px", backgroundColor: "blue" }}>
                <SizeGetter
                    render={({ width, height }) => (
                        <div>
                            Parent size
                            <br />
                            width: {width}
                            <br />
                            height: {height}
                        </div>
                    )}
                />
            </div>
        );
    },
};
