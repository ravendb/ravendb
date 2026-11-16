import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { SharedStubs } from "test/stubs/SharedStubs";
import ConnectionTestErrorModal from "./ConnectionTestErrorModal";

interface ConnectionTestErrorModalStoryArgs {
    hasLongValues: boolean;
}

const LONG_ERROR =
    "Amazon.S3.AmazonS3Exception: The AWS Access Key Id you provided does not exist in our records for the bucket ravendb-production-europe-orders-nightly-backups-bucket-name-long.\n" +
    "   at Amazon.Runtime.Internal.HttpErrorResponseExceptionHandler.HandleExceptionStream(IRequestContext requestContext, IWebResponseData httpErrorResponse, HttpErrorResponseException exception, Stream responseStream)\n".repeat(
        30
    );

export default {
    title: "Bits/ConnectionTestErrorModal",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    args: {
        hasLongValues: false,
    },
    render: (args) => (
        <ConnectionTestErrorModal
            error={args.hasLongValues ? LONG_ERROR : SharedStubs.nodeConnectionTestErrorResult().Error}
            onClose={() => {}}
        />
    ),
} satisfies Meta<ConnectionTestErrorModalStoryArgs>;

type Story = StoryObj<ConnectionTestErrorModalStoryArgs>;

export const Default: Story = {};

export const LongValues: Story = {
    args: { hasLongValues: true },
};
