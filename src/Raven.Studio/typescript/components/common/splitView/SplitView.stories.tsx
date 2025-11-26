import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import { useViewSheet, ViewSheet } from "./ViewSheet";
import Button from "react-bootstrap/Button";
import { ViewSheetWidth } from "./ViewSheet";
import { Icon } from "components/common/Icon";

export default {
    title: "Bits/SplitView",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface DefaultStoryArgs {
    initialWidth: ViewSheetWidth;
    minWidth: ViewSheetWidth;
    maxWidth: ViewSheetWidth;
}

export const Default: StoryObj<DefaultStoryArgs> = {
    name: "SplitView",
    render: (args) => {
        const { open, close } = useViewSheet();

        const handleOpenSheet = () => {
            open({
                component: (
                    <ViewSheet>
                        <ViewSheet.Header>
                            <h5 className="mb-0">
                                <Icon icon="document" />
                                Sheet header
                            </h5>
                        </ViewSheet.Header>
                        <ViewSheet.Body>
                            <span>Sheet body</span>
                        </ViewSheet.Body>
                        <ViewSheet.Footer>
                            <span>Sheet footer</span>
                        </ViewSheet.Footer>
                    </ViewSheet>
                ),
                ...args,
            });
        };

        return (
            <div className="vstack gap-2">
                <div>Main component</div>
                <p>
                    Some long text Some long text Some long text Some long text Some long text Some long text Some long
                    text Some long text Some long text Some long text Some long text Some long text Some long text Some
                    long text Some long text
                </p>
                <div>
                    <Button variant="primary" onClick={handleOpenSheet}>
                        Open Sheet
                    </Button>
                </div>
                <div>
                    <Button variant="secondary" onClick={close}>
                        Close Sheet
                    </Button>
                </div>
            </div>
        );
    },
    args: {
        initialWidth: "50%",
        minWidth: "30%",
        maxWidth: "75%",
    },
};
