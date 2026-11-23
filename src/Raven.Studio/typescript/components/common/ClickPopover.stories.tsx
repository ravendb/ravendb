import { Meta, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import Button from "react-bootstrap/Button";
import { userEvent } from "storybook/test";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { ClickPopover } from "./ClickPopover";
import { Icon } from "./Icon";

interface ClickPopoverStoryArgs {
    content: string;
    containerWidth: number | null;
}

const SHORT_CONTENT = "Process is in error recovery. Next retry in 16 minutes.";

const LONG_CONTENT =
    "Process is in error recovery (fallback mode). Last error: 2026-11-23T08:34:44.7081752Z. Next retry in: 971s. " +
    "System.Net.Sockets.SocketException (10061): No connection could be made because the target machine actively refused it.";

export default {
    title: "Bits/ClickPopover",
    decorators: [withStorybookContexts, withBootstrap5],
    render: ({ content, containerWidth }: ClickPopoverStoryArgs) => (
        <div className="p-4" style={{ width: containerWidth ?? undefined }}>
            <ClickPopover content={content}>
                <Button
                    variant="link"
                    size="sm"
                    className="d-flex align-items-center p-0 text-warning"
                    title="Show details"
                >
                    <Icon icon="warning" margin="me-1" />
                    Reconnect
                </Button>
            </ClickPopover>
        </div>
    ),
    args: {
        content: SHORT_CONTENT,
        containerWidth: null,
    },
    play: async ({ canvas }) => {
        await userEvent.click(await canvas.findByTitle("Show details"));
    },
} satisfies Meta<ClickPopoverStoryArgs>;

export const Default: StoryObj<ClickPopoverStoryArgs> = {};

export const LongText: StoryObj<ClickPopoverStoryArgs> = {
    args: {
        content: LONG_CONTENT,
    },
};

export const Narrow: StoryObj<ClickPopoverStoryArgs> = {
    args: {
        content: LONG_CONTENT,
        containerWidth: 200,
    },
};
