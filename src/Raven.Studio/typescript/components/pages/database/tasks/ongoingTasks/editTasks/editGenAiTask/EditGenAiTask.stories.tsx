import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import EditGenAiTask from "./EditGenAiTask";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { userEvent } from "@storybook/test";
import { EditGenAiTaskStepId } from "./hooks/useEditGenAiTaskSteps";
import { Canvas } from "storybook/internal/csf";

export default {
    title: "Pages/Tasks/Ongoing Tasks/Edit tasks/GenAI",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/OvnqnwTMCiznsaTUxZHiP6/Pages---Gen-AI",
        },
        test: {
            dangerouslyIgnoreUnhandledErrors: true,
        },
    },
} satisfies Meta;

export const Basic: StoryObj = {
    render: () => {
        const { cluster, collectionsTracker, license } = mockStore;
        const { tasksService } = mockServices;

        cluster.with_Single();
        collectionsTracker.with_Collections();

        tasksService.withTestAiConnectionString();
        tasksService.withConnectionStrings();
        tasksService.withTestGenAi();

        license.with_License({
            HasGenAi: true,
        });

        return (
            <div style={{ height: "90vh" }}>
                <EditGenAiTask />
            </div>
        );
    },
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "basic");
    },
};

export const Context: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "context");
    },
};

export const ModelInput: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "modelInput");
    },
};

export const UpdateScript: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "updateScript");
    },
};

export const Summary: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "summary");
    },
};

async function navigateToStep(canvas: Canvas, step: EditGenAiTaskStepId) {
    await userEvent.type(await canvas.findByLabelText("Task Name"), "some-name");
    await userEvent.click(canvas.getByText("Select..."));
    await userEvent.click(canvas.getByText("ai-name"));

    if (step === "basic") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("Select...")[0]);
    await userEvent.click(canvas.getByText("Orders"));
    await userEvent.click(canvas.getByText("1"));
    await userEvent.paste(sampleContextScript);
    await userEvent.click(canvas.getByRole("button", { name: "Or enter a document manually" }));
    await userEvent.click(canvas.getByRole("button", { name: "Enter edit mode" }));
    await userEvent.click(canvas.getByRole("button", { name: "Test context" }));

    if (step === "context") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("1")[0]);
    await userEvent.paste(samplePrompt);
    await userEvent.click(canvas.getByText("Sample response object"));
    await userEvent.click(canvas.getAllByText("1")[1]);
    await userEvent.paste(sampleObject);
    await userEvent.click(canvas.getByRole("button", { name: "Test model" }));

    if (step === "modelInput") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("1")[0]);
    await userEvent.paste(sampleUpdateScript);
    await userEvent.click(canvas.getByRole("button", { name: "Test update script" }));

    if (step === "updateScript") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
}

const sampleContextScript = `for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}`;

const samplePrompt =
    "Check if the following blog post comment is spam or not. A spam comment typically includes irrelevant or promotional content, excessive links, misleading information, or is written with the intent to manipulate search rankings or advertise products/services. Consider the language, intent, and relevance of the comment to the blog post topic. ";

const sampleObject = `{
    "Blocked": true,
    "Reason": "Concise reason for why this comment was marked as spam or ham"
}`;

const sampleUpdateScript = `const contextObj = $input;
const resultObj = $output;

const idx = this.Comments.findIndex(comment => comment.Id == contextObj.Id);

if (resultObj.Blocked) {
    this.Comments.splice(idx, 1); // remove
}`;
