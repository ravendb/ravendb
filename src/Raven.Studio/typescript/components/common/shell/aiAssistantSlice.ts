import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createLoadingState, createSuccessState } from "components/utils/common";
import { services } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";
import { CheckConsentAiAssistantResultDto } from "commands/aiAssistant/checkConsentAiAssistantCommand";
import { CheckUsageAiAssistantResultDto } from "commands/aiAssistant/checkUsageAiAssistantCommand";
import { aiAssistantConstants } from "../aiAssistant/aiAssistantConstants";

interface AiAssistantSettings {
    isDisabled: boolean;
    isDataSubmissionDisabled: boolean;
}

interface AiAssistantState {
    consentStatus: loadableData<CheckConsentAiAssistantResultDto["Status"]>;
    usage: loadableData<CheckUsageAiAssistantResultDto>;
    settings: AiAssistantSettings;
}

const initialState: AiAssistantState = {
    consentStatus: createIdleState("ConsentRequired"),
    usage: createIdleState(),
    settings: {
        isDisabled: false,
        isDataSubmissionDisabled: false,
    },
};

export const aiAssistantSlice = createSlice({
    name: "aiAssistant",
    initialState,
    reducers: {
        consentStatusSet: (state, action: PayloadAction<CheckConsentAiAssistantResultDto["Status"]>) => {
            state.consentStatus = createSuccessState(action.payload);
        },
        usagePercentageSet: (state, action: PayloadAction<number>) => {
            state.usage = createSuccessState({
                Status: "Success",
                UsagePercentage: action.payload,
            });
        },
        settingsSet: (state, action: PayloadAction<AiAssistantSettings>) => {
            state.settings = action.payload;
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(checkConsent.pending, (state) => {
                state.consentStatus = createLoadingState();
            })
            .addCase(checkConsent.rejected, (state, action) => {
                state.consentStatus = createFailureState(action.error.message);
            })
            .addCase(checkConsent.fulfilled, (state, action) => {
                state.consentStatus = createSuccessState(action.payload.Status);
            })
            .addCase(checkUsage.pending, (state) => {
                state.usage = createLoadingState();
            })
            .addCase(checkUsage.rejected, (state, action) => {
                state.usage = createFailureState(action.error.message);
            })
            .addCase(checkUsage.fulfilled, (state, action) => {
                state.usage = createSuccessState(action.payload);
            })
            .addCase(giveConsent.fulfilled, (state, action) => {
                state.consentStatus = createSuccessState(action.payload);
            });
    },
});

const checkConsent = createAsyncThunk(
    aiAssistantSlice.name + "/checkConsent",
    async () => await services.aiAssistantService.checkConsent()
);

const checkUsage = createAsyncThunk(
    aiAssistantSlice.name + "/checkUsage",
    async () => await services.aiAssistantService.checkUsage()
);

const giveConsent = createAsyncThunk(aiAssistantSlice.name + "/giveConsent", async () => {
    const result = await services.aiAssistantService.giveConsent();

    if (result.Status === "InvalidCredentials") {
        messagePublisher.reportError(
            `Failed to give consent to AI Assistant. ${aiAssistantConstants.invalidCredentials}`
        );
    }

    return result.Status;
});

export const aiAssistantActions = {
    ...aiAssistantSlice.actions,
    checkConsent,
    giveConsent,
    checkUsage,
};

export const aiAssistantSelectors = {
    consentStatus: (store: RootState) => store.aiAssistant.consentStatus,
    isConsentSuccess: (store: RootState) => store.aiAssistant.consentStatus.data === "Success",
    usage: (store: RootState) => store.aiAssistant.usage,
    isDataSubmissionDisabled: (store: RootState) => store.aiAssistant.settings.isDataSubmissionDisabled,
    isDisabled: (store: RootState) => store.aiAssistant.settings.isDisabled || !store.license.status?.HasAiAssistant,
    settings: (store: RootState) => store.aiAssistant.settings,
};
