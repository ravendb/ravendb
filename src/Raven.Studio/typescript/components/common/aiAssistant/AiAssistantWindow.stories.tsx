import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import AiAssistantWindow from "./AiAssistantWindow";
import { mockServices } from "test/mocks/services/MockServices";
import { useState } from "react";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";

export default {
    title: "Bits/AI Assistant Window",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface AiAssistantWindowStoryArgs {
    consentStatus: CheckConsentAiAssistantResultDto["Status"];
    assistStatus: AiAssistantResponseStatus;
}

export const AiAssistantWindowStory: StoryObj<AiAssistantWindowStoryArgs> = {
    name: "AI Assistant Window",
    render: (args) => {
        const { aiAssistantService } = mockServices;

        aiAssistantService.withCheckConsent({ Status: args.consentStatus });
        aiAssistantService.withAssist((dto) => {
            dto.Status = args.assistStatus;
        });

        return <AiAssistantWindowStoryComponent />;
    },
    args: {
        consentStatus: "Success",
        assistStatus: "Success",
    },
    argTypes: {
        consentStatus: {
            control: "select",
            options: ["Success", "InvalidCredentials", "ConsentRequired"],
        },
        assistStatus: {
            control: "select",
            options: ["Success", "InvalidCredentials", "InvalidData", "OutOfTokens"],
        },
    },
};

function AiAssistantWindowStoryComponent() {
    const [assistantResultText, setAssistantResultText] = useState("");

    return (
        <div>
            <div className="position-relative" style={{ width: "500px", height: "250px" }}>
                <AiAssistantWindow
                    closeWindow={() => {}}
                    acceptResult={setAssistantResultText}
                    data={{ OperationType: "RefinePrompt", View: "AI Agents", Message: textToRefine }}
                    successMessage="AI refined your prompt based on your input."
                />
            </div>

            <div>
                <b>Text to refine:</b> {textToRefine}
            </div>
            <div>
                <b>Accepted text:</b> {assistantResultText || "-"}
            </div>
        </div>
    );
}

const textToRefine = "you are an agent used to help with finding orders";
