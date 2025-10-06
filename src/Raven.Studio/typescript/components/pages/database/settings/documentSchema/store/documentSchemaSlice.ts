import { createEntityAdapter, createSlice, EntityState, PayloadAction, nanoid } from "@reduxjs/toolkit";

import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;

export interface DocumentSchemaValidatorConfig
    extends Raven.Client.Documents.Operations.SchemaValidation.SchemaDefinition {
    Name: string;
}

export interface DocumentSchemaState {
    selectedCollectionNames: string[];
    validators: EntityState<DocumentSchemaValidatorConfig, string>;
    originalValidators: EntityState<DocumentSchemaValidatorConfig, string>;
    newDraftIds: string[];
    globalDisabled: boolean;
}

const validatorsAdapter = createEntityAdapter<DocumentSchemaValidatorConfig, string>({
    selectId: (config) => config.Name,
});

const validatorsSelectors = validatorsAdapter.getSelectors();

const initialState: DocumentSchemaState = {
    selectedCollectionNames: [],
    validators: validatorsAdapter.getInitialState(),
    originalValidators: validatorsAdapter.getInitialState(),
    newDraftIds: [],
    globalDisabled: false,
};

export const documentSchemaSlice = createSlice({
    name: "documentSchema",
    initialState,
    reducers: {
        validatorsLoadedFromServer: (state, { payload }: PayloadAction<SchemaValidationConfiguration>) => {
            const map = payload?.ValidatorsPerCollection ?? {};

            const items: DocumentSchemaValidatorConfig[] = Object.keys(map).map((name) => ({
                Name: name,
                Disabled: map[name].Disabled ?? false,
                Schema: map[name].Schema ?? "{}",
                LastModifiedTime: map[name].LastModifiedTime,
            }));

            validatorsAdapter.setAll(state.originalValidators, items);
            validatorsAdapter.setAll(state.validators, items);
            state.globalDisabled = payload?.Disabled ?? false;
        },
        validatorAdded: (state, { payload }: PayloadAction<DocumentSchemaValidatorConfig>) => {
            validatorsAdapter.addOne(state.validators, payload);
        },
        validatorEdited: (
            state,
            { payload }: PayloadAction<{ originalName: string; validator: DocumentSchemaValidatorConfig }>
        ) => {
            if (payload.originalName !== payload.validator.Name) {
                validatorsAdapter.removeOne(state.validators, payload.originalName);
                validatorsAdapter.addOne(state.validators, payload.validator);
            } else {
                validatorsAdapter.updateOne(state.validators, {
                    id: payload.validator.Name,
                    changes: payload.validator,
                });
            }
        },
        validatorDeleted: (state, { payload: name }: PayloadAction<string>) => {
            validatorsAdapter.removeOne(state.validators, name);
        },
        allSelectedCollectionNamesToggled: (state) => {
            if (state.selectedCollectionNames.length === 0) {
                state.selectedCollectionNames = validatorsSelectors.selectIds(state.validators);
            } else {
                state.selectedCollectionNames = [];
            }
        },
        selectedCollectionNameToggled: (state, { payload: name }: PayloadAction<string>) => {
            if (state.selectedCollectionNames.includes(name)) {
                state.selectedCollectionNames = state.selectedCollectionNames.filter((n) => n !== name);
            } else {
                state.selectedCollectionNames.push(name);
            }
        },
        selectedValidatorsDeleted: (state) => {
            validatorsAdapter.removeMany(state.validators, state.selectedCollectionNames);
            state.selectedCollectionNames = [];
        },
        draftAdded: (state, { payload }: PayloadAction<string | undefined>) => {
            state.newDraftIds.push(payload ?? nanoid());
        },
        draftRemoved: (state, { payload: id }: PayloadAction<string>) => {
            state.newDraftIds = state.newDraftIds.filter((x) => x !== id);
        },
        validatorsSaved: (state) => {
            validatorsAdapter.setAll(state.originalValidators, validatorsSelectors.selectAll(state.validators));
        },
        globalDisabledToggled: (state, { payload: disabled }: PayloadAction<boolean>) => {
            state.globalDisabled = disabled;
        },
        reset: () => initialState,
    },
    extraReducers: () => {},
});

export const documentSchemaActions = {
    ...documentSchemaSlice.actions,
};

export const documentSchemaSliceInternal = {
    validatorsSelectors,
};
