import { aiAssistantActions } from "components/common/shell/aiAssistantSlice";
import { globalDispatch } from "components/storeCompat";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";

export class MockAiAssistant {
    with_consent(status: CheckConsentAiAssistantResultDto["Status"]) {
        globalDispatch(aiAssistantActions.consentStatusSet(status));
    }
}
