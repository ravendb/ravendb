import { useForm } from "react-hook-form";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { expect, waitFor } from "storybook/test";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import FormCronEditor from "./FormCronEditor";

interface FormCronEditorStoryArgs {
    expression: string;
    isRejectedByServer: boolean;
    containerWidth: number;
}

interface FormCronEditorStoryFormData {
    frequency: string;
}

function FormCronEditorStory({ expression, isRejectedByServer, containerWidth }: FormCronEditorStoryArgs) {
    mockServices.tasksService.withCronExpressionOccurrence((occurrence) => {
        if (isRejectedByServer) {
            occurrence.IsValid = false;
            occurrence.ErrorMessage = "The server does not support this cron expression";
        }
    });

    const { control } = useForm<FormCronEditorStoryFormData>({ defaultValues: { frequency: expression } });

    return (
        <div className="panel-bg-1 border border-color-light rounded-2 p-3" style={{ maxWidth: containerWidth }}>
            <FormCronEditor control={control} name="frequency" />
        </div>
    );
}

export default {
    title: "Bits/FormCronEditor",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    args: {
        expression: "0 2 * * 0",
        isRejectedByServer: false,
        containerWidth: 926,
    },
    render: (args) => <FormCronEditorStory {...args} />,
    play: async ({ canvasElement }) => {
        await waitFor(() => expect(canvasElement.querySelector(".spinner-border")).toBeNull());
    },
} satisfies Meta<FormCronEditorStoryArgs>;

type Story = StoryObj<FormCronEditorStoryArgs>;

export const Default: Story = {};

export const Empty: Story = {
    args: { expression: "" },
};

export const YearlySchedule: Story = {
    args: { expression: "30 4 1 6 *" },
};

export const HourlySchedule: Story = {
    args: { expression: "15 * * * *" },
};

export const CustomMinute: Story = {
    args: { expression: "2 * * * *" },
};

export const Advanced: Story = {
    args: { expression: "0 */6 * * 1-5" },
};

export const InvalidExpression: Story = {
    args: { expression: "0 2 * *" },
};

export const RejectedByServer: Story = {
    args: { expression: "0 */6 * * 1-5", isRejectedByServer: true },
};

export const Narrow: Story = {
    args: { expression: "30 4 1 6 *", containerWidth: 320 },
};
