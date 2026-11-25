import { FormProvider, useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent, within } from "storybook/test";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { createTranslator } from "common/i18n/i18n";
import DestinationPanel from "./DestinationPanel";
import { DestinationType, FormDestinations } from "./utils/formDestinationsTypes";
import { mapDestinationsFromDto } from "./utils/formDestinationsMapsFromDto";
import { destinationsSchema } from "./utils/formDestinationsValidation";

const t = createTranslator("formDestinations");

interface DestinationPanelStoryArgs {
    type: DestinationType;
    isDisabled: boolean;
    state: "none" | "enabled" | "disabled";
    isEnableAllowed: boolean;
    hasLongValues: boolean;
    hasInvalidSettings: boolean;
    containerWidth: number;
}

function getDestinations({ type, state, hasLongValues, hasInvalidSettings }: DestinationPanelStoryArgs) {
    const { destinations } = mapDestinationsFromDto(TasksStubs.periodicBackupConfiguration());

    if (state === "disabled") {
        destinations[type].isEnabled = false;
    }

    if (hasLongValues) {
        destinations.local.folderPath =
            "/var/lib/ravendb/backups/production/europe/orders/nightly/full-and-incremental-backups-folder-with-a-very-long-name";
        destinations.s3.bucketName = "ravendb-production-europe-orders-nightly-backups-bucket-name-long";
        destinations.s3.remoteFolderName = "production/europe/orders/nightly/remote-folder-with-a-very-long-name";
        destinations.azure.config.arguments =
            "--environment production --region europe-west-1 --vault orders-nightly-backups --verbose --retry 5";
    }

    if (hasInvalidSettings) {
        destinations.s3.bucketName = "Invalid Bucket Name";
    }

    return destinations;
}

function DestinationPanelStory(args: DestinationPanelStoryArgs) {
    mockServices.manageServerService.withTestPeriodicBackupCredentials();

    const form = useForm<FormDestinations>({
        resolver: yupResolver(destinationsSchema),
        defaultValues: { destinations: getDestinations(args) },
    });

    const setState = (isEnabled: boolean) => {
        const name = `destinations.${args.type}` as const;
        form.setValue(name, { ...form.getValues(name), isEnabled });
    };

    return (
        <FormProvider {...form}>
            <div style={{ maxWidth: args.containerWidth }}>
                <DestinationPanel
                    type={args.type}
                    isDisabled={args.isDisabled}
                    allowedDestinations={args.isEnableAllowed ? undefined : []}
                    onEdit={() => null}
                    onDelete={() => null}
                    onStateChange={args.state === "none" ? undefined : setState}
                />
            </div>
        </FormProvider>
    );
}

export default {
    title: "Bits/FormDestinations/DestinationPanel",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    argTypes: {
        type: { control: "radio", options: ["local", "s3", "azure"] },
        state: { control: "radio", options: ["none", "enabled", "disabled"] },
    },
    args: {
        type: "s3",
        isDisabled: false,
        state: "none",
        isEnableAllowed: true,
        hasLongValues: false,
        hasInvalidSettings: false,
        containerWidth: 926,
    },
    render: (args) => <DestinationPanelStory {...args} />,
} satisfies Meta<DestinationPanelStoryArgs>;

type Story = StoryObj<DestinationPanelStoryArgs>;

export const Default: Story = {};

export const Local: Story = {
    args: { type: "local" },
};

export const ExternalScript: Story = {
    args: { type: "azure" },
};

export const Disabled: Story = {
    args: { isDisabled: true },
};

export const StateEnabled: Story = {
    args: { state: "enabled" },
};

export const StateDisabled: Story = {
    args: { state: "disabled" },
};

export const StateEnableNotAllowed: Story = {
    args: { state: "disabled", isEnableAllowed: false },
};

export const StateNarrow: Story = {
    args: { state: "disabled", hasLongValues: true, containerWidth: 420 },
};

const testCredentials: Story["play"] = async ({ canvasElement }) => {
    await userEvent.click(await within(canvasElement).findByRole("button", { name: t("testCredentials") }));
};

export const InvalidSettings: Story = {
    args: { hasInvalidSettings: true },
    play: testCredentials,
};

export const TestSucceeded: Story = {
    play: testCredentials,
};

export const TestSucceededNarrow: Story = {
    args: { state: "disabled", hasLongValues: true, containerWidth: 420 },
    play: testCredentials,
};

export const LongValues: Story = {
    args: { hasLongValues: true },
};

export const Narrow: Story = {
    args: { hasLongValues: true, containerWidth: 420 },
};
