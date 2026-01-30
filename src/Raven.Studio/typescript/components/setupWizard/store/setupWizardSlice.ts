import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface SetupWizardState {
    eulaStep: {
        isEulaScrolledToBottom: boolean;
    };
    finishStep: {
        status: OperationStatus;
    };
    selfSignedCertificateStep: {
        isPasswordValid: boolean;
        hasPassword: boolean;
    };
}

const initialState: SetupWizardState = {
    eulaStep: {
        isEulaScrolledToBottom: false,
    },
    finishStep: {
        status: "InProgress",
    },
    selfSignedCertificateStep: {
        isPasswordValid: true,
        hasPassword: false,
    },
};

export const setupWizardSlice = createSlice({
    name: "setupWizard",
    initialState,
    reducers: {
        isEulaScrolledToBottomSet: (state, action: PayloadAction<boolean>) => {
            state.eulaStep.isEulaScrolledToBottom = action.payload;
        },
        finishStepStatusSet: (state, action: PayloadAction<OperationStatus>) => {
            state.finishStep.status = action.payload;
        },
        selfSignedCertificateStepIsPasswordValidSet: (state, action: PayloadAction<boolean>) => {
            state.selfSignedCertificateStep.isPasswordValid = action.payload;
        },
        selfSignedCertificateStepHasPasswordSet: (state, action: PayloadAction<boolean>) => {
            state.selfSignedCertificateStep.hasPassword = action.payload;
        },
    },
});

export const setupWizardActions = setupWizardSlice.actions;

export const setupWizardSelectors = {
    isEulaScrolledToBottom: (state: RootState) => state.setupWizard.eulaStep.isEulaScrolledToBottom,
    finishStepStatus: (state: RootState) => state.setupWizard.finishStep.status,
    selfSignedCertificateStepIsPasswordValid: (state: RootState) =>
        state.setupWizard.selfSignedCertificateStep.isPasswordValid,
    selfSignedCertificateStepHasPassword: (state: RootState) => state.setupWizard.selfSignedCertificateStep.hasPassword,
};
