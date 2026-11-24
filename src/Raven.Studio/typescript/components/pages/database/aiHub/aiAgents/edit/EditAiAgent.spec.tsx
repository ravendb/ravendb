import { rtlRender_WithWaitForLoad } from "test/rtlTestUtils";
import * as Stories from "./EditAiAgent.stories";
import { composeStories } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";
import { waitFor } from "@testing-library/react";

const { Default, CustomAdvancedSettings } = composeStories(Stories);

const selectors = {
    maxModelIterationsLabel: /Max model iterations per call/,
    beginningPromptLabel: /Summarization beginning prompt/,
    endPromptLabel: /Summarization end prompt/,
    summaryPrefixLabel: /Summary prefix/,
    defaultPlaceholder: "Default (from the configuration)",
};

const customBeginningPrompt =
    "Summarize this schema migration planning session.\nPreserve every table name and every decision.";

type RenderResult = Awaited<ReturnType<typeof rtlRender_WithWaitForLoad>>;

function getChatTrimmingAdvancedSettingsButton(screen: RenderResult["screen"]) {
    const advancedSettingsButtons = screen.getAllByRole("button", { name: "Advanced settings" });
    return advancedSettingsButtons[advancedSettingsButtons.length - 1];
}

async function openChatTrimmingAdvancedSettings({ screen, fireClick }: Pick<RenderResult, "screen" | "fireClick">) {
    await fireClick(getChatTrimmingAdvancedSettingsButton(screen));

    expect(getChatTrimmingAdvancedSettingsButton(screen)).toHaveAttribute("aria-expanded", "true");
}

describe("EditAiAgent", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("shows the iterations limit and the summarization prompts of an existing agent", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<CustomAdvancedSettings />);
        await openChatTrimmingAdvancedSettings({ screen, fireClick });

        expect(screen.getByLabelText(selectors.maxModelIterationsLabel)).toHaveValue(30);
        expect(screen.getByLabelText(selectors.beginningPromptLabel)).toHaveValue(customBeginningPrompt);
        expect(screen.getByLabelText(selectors.endPromptLabel)).toHaveValue(
            "Summarize the conversation above under those rules."
        );
        expect(screen.getByLabelText(selectors.summaryPrefixLabel)).toHaveValue("Planning session so far: ");
    });

    it("leaves the iterations limit and the summarization prompts empty for a new agent", async () => {
        const { screen, fireClick } = await rtlRender_WithWaitForLoad(<Default />);
        await openChatTrimmingAdvancedSettings({ screen, fireClick });

        expect(screen.getByLabelText(selectors.maxModelIterationsLabel)).toHaveValue(null);
        expect(screen.getByLabelText(selectors.beginningPromptLabel)).toHaveValue("");
        expect(screen.getByLabelText(selectors.beginningPromptLabel)).toHaveAttribute(
            "placeholder",
            selectors.defaultPlaceholder
        );
        expect(screen.getByLabelText(selectors.endPromptLabel)).toHaveValue("");
        expect(screen.getByLabelText(selectors.summaryPrefixLabel)).toHaveValue("");
    });

    it("saves the changed iterations limit and prefix together with the prompts that were not changed", async () => {
        const { screen, fireClick, fillInput } = await rtlRender_WithWaitForLoad(<CustomAdvancedSettings />);
        const saveAiAgent = mockServices.aiAgentService.getMock("saveAiAgent");

        await fillInput(screen.getByLabelText(selectors.maxModelIterationsLabel), "5");
        await openChatTrimmingAdvancedSettings({ screen, fireClick });
        await fillInput(screen.getByLabelText(selectors.summaryPrefixLabel), "Earlier in this chat:");
        await fireClick(screen.getByRole("button", { name: /Save/ }));
        await waitFor(() => expect(saveAiAgent).toHaveBeenCalled());

        const savedAgent = saveAiAgent.mock.calls[0][1];

        expect(savedAgent.MaxModelIterationsPerCall).toBe(5);
        expect(savedAgent.ChatTrimming.Tokens).toMatchObject({
            SummarizationTaskBeginningPrompt: customBeginningPrompt,
            SummarizationTaskEndPrompt: "Summarize the conversation above under those rules.",
            ResultPrefix: "Earlier in this chat:",
        });
    });
});
