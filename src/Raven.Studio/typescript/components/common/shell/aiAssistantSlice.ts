import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";
import { services } from "components/hooks/useServices";
import messagePublisher from "common/messagePublisher";

interface AiAssistantState {
    consentStatus: loadableData<AiAssistantResponseStatus>;
}

const initialState: AiAssistantState = {
    consentStatus: createIdleState("ConsentRequired"),
};

export const aiAssistantSlice = createSlice({
    name: "aiAssistant",
    initialState,
    reducers: {},
    extraReducers: (builder) => {
        builder
            .addCase(checkConsent.pending, (state) => {
                state.consentStatus.status = "loading";
            })
            .addCase(checkConsent.rejected, (state, action) => {
                state.consentStatus = createFailureState(action.error.message);
            })
            .addCase(checkConsent.fulfilled, (state, action) => {
                state.consentStatus = createSuccessState(action.payload.Status);
            });
    },
});

const checkConsent = createAsyncThunk(
    aiAssistantSlice.name + "/checkConsent",
    services.aiAssistantService.checkConsent
);

const giveConsent = createAsyncThunk(aiAssistantSlice.name + "/giveConsent", async () => {
    const result = await services.aiAssistantService.giveConsent();

    if (result.Status === "InvalidCredentials") {
        messagePublisher.reportError("Failed to give consent to AI Assistant. Invalid credentials.");
        return;
    }

    if (result.Status !== "Success") {
        messagePublisher.reportError("Failed to give consent to AI Assistant.");
        return;
    }

    checkConsent();
});

export const aiAssistantActions = {
    ...aiAssistantSlice.actions,
    checkConsent,
    giveConsent,
};

export const aiAssistantSelectors = {
    consentStatus: (store: RootState) => store.aiAssistant.consentStatus,
};
