import { FormProvider, useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent, within } from "storybook/test";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { SharedStubs } from "test/stubs/SharedStubs";
import { TasksStubs } from "test/stubs/TasksStubs";
import { createTranslator } from "common/i18n/i18n";
import AmazonS3 from "./AmazonS3";
import { FormDestinations } from "./utils/formDestinationsTypes";
import { mapDestinationsFromDto } from "./utils/formDestinationsMapsFromDto";
import { destinationsSchema } from "./utils/formDestinationsValidation";

const t = createTranslator("formDestinations");

interface DestinationCardStoryArgs {
    isTestFailing: boolean;
    containerWidth: number;
}

function DestinationCardStory({ isTestFailing, containerWidth }: DestinationCardStoryArgs) {
    mockServices.manageServerService.withTestPeriodicBackupCredentials(
        isTestFailing ? SharedStubs.nodeConnectionTestErrorResult() : undefined
    );

    const form = useForm<FormDestinations>({
        resolver: yupResolver(destinationsSchema),
        defaultValues: { destinations: mapDestinationsFromDto(TasksStubs.periodicBackupConfiguration()).destinations },
    });

    return (
        <FormProvider {...form}>
            <div style={{ maxWidth: containerWidth }}>
                <AmazonS3 />
            </div>
        </FormProvider>
    );
}

export default {
    title: "Bits/FormDestinations/DestinationCard",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    args: {
        isTestFailing: false,
        containerWidth: 926,
    },
    render: (args) => <DestinationCardStory {...args} />,
} satisfies Meta<DestinationCardStoryArgs>;

type Story = StoryObj<DestinationCardStoryArgs>;

const testCredentials: Story["play"] = async ({ canvasElement }) => {
    await userEvent.click(await within(canvasElement).findByRole("button", { name: t("testCredentials") }));
};

export const Default: Story = {};

export const TestSucceeded: Story = {
    play: testCredentials,
};

export const TestFailed: Story = {
    args: { isTestFailing: true },
    play: testCredentials,
};

export const Narrow: Story = {
    args: { containerWidth: 420 },
    play: testCredentials,
};
