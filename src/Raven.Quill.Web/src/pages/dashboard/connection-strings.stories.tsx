import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, screen, userEvent, waitFor, within } from "storybook/test";
import {
    aiConnectionStringsMocks,
    sampleUsedByAgentsConnectionStringResponses,
    sampleUsedByTasksConnectionStringResponses,
    sampleUsedByTasksVertexConnectionStringResponses,
} from "@/mocks/ai-connection-strings-mocks";
import { DashboardConnectionStrings } from "./connection-strings";

const meta = {
    title: "Dashboard/Connection strings",
    component: DashboardConnectionStrings,
    parameters: {
        // The page renders its own "AI connection strings" header with the add
        // button beside it, so the shell decorator adds no title.
        page: {},
    },
} satisfies Meta<typeof DashboardConnectionStrings>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [aiConnectionStringsMocks.list([])],
            },
        },
    },
};

export const UsedByAgents: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [
                    aiConnectionStringsMocks.list(sampleUsedByAgentsConnectionStringResponses),
                    aiConnectionStringsMocks.detail(sampleUsedByAgentsConnectionStringResponses),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(await canvas.findByRole("button", { name: "Delete openai-chat" })).toHaveAttribute(
            "aria-disabled",
            "true",
        );

        await userEvent.click(canvas.getByRole("button", { name: "Edit openai-chat" }));
        const sheet = within(await screen.findByRole("dialog"));
        await waitFor(() => expect(sheet.getByText("In use")).toBeVisible());
        expect(sheet.getByLabelText("Model")).toBeEnabled();
        expect(sheet.getByLabelText("Provider")).toBeEnabled();
    },
};

export const UsedByTasks: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [
                    aiConnectionStringsMocks.list(sampleUsedByTasksConnectionStringResponses),
                    aiConnectionStringsMocks.detail(sampleUsedByTasksConnectionStringResponses),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(await canvas.findByRole("button", { name: "Delete openai-chat" })).toHaveAttribute(
            "aria-disabled",
            "true",
        );

        await userEvent.click(canvas.getByRole("button", { name: "Edit openai-chat" }));
        const sheet = within(await screen.findByRole("dialog"));
        await waitFor(() => expect(sheet.getByLabelText("Model")).toBeDisabled());
        expect(sheet.getByLabelText("Provider")).toBeDisabled();
        expect(sheet.getByLabelText("API Key")).toBeEnabled();
    },
};

// Vertex locks Location as well: RavenDB counts it as a deployment change that would
// invalidate the embeddings the task has already written.
export const UsedByTasksVertex: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [
                    aiConnectionStringsMocks.list(sampleUsedByTasksVertexConnectionStringResponses),
                    aiConnectionStringsMocks.detail(sampleUsedByTasksVertexConnectionStringResponses),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("button", { name: "Edit vertex-embeddings" }));
        const sheet = within(await screen.findByRole("dialog"));
        await waitFor(() => expect(sheet.getByLabelText("Model")).toBeDisabled());
        expect(sheet.getByLabelText("Location")).toBeDisabled();
        expect(sheet.getByLabelText("Google Credentials JSON")).toBeEnabled();
    },
};
