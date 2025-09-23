import { createEntityAdapter, createSlice, EntityState, PayloadAction, nanoid } from "@reduxjs/toolkit";

// Raven types
import SchemaValidationConfiguration = Raven.Client.Documents.Operations.SchemaValidation.SchemaValidationConfiguration;
export type CollectionName = string & NonNullable<unknown>;

export interface DocumentSchemaValidatorConfig
    extends Raven.Client.Documents.Operations.SchemaValidation.SchemaDefinition {
    Name: CollectionName;
}

export interface DocumentSchemaState {
    selectedCollectionNames: CollectionName[];
    validators: EntityState<DocumentSchemaValidatorConfig, string>;
    originalValidators: EntityState<DocumentSchemaValidatorConfig, string>;
    newDraftIds: string[]; // IDs for unsaved draft cards
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
};

export const documentSchemaSlice = createSlice({
    name: "documentSchema",
    initialState,
    reducers: {
        validatorsLoadedFromServer: (state, { payload }: PayloadAction<SchemaValidationConfiguration>) => {
            const map = (payload?.ValidatorsPerCollection ??
                {}) as SchemaValidationConfiguration["ValidatorsPerCollection"];

            const items: DocumentSchemaValidatorConfig[] = Object.keys(map).map((name) => ({
                Name: name,
                Disabled: map[name].Disabled ?? false,
                Schema: map[name].Schema ?? "{}",
                LastModifiedTime: map[name].LastModifiedTime,
            }));

            validatorsAdapter.setAll(state.originalValidators, items);
            validatorsAdapter.setAll(state.validators, items);
        },
        validatorAdded: (state, { payload }: PayloadAction<DocumentSchemaValidatorConfig>) => {
            validatorsAdapter.addOne(state.validators, payload);
        },
        validatorEdited: (state, { payload }: PayloadAction<DocumentSchemaValidatorConfig>) => {
            validatorsAdapter.updateOne(state.validators, {
                id: payload.Name,
                changes: { ...payload },
            });
        },
        validatorDeleted: (state, { payload: name }: PayloadAction<CollectionName>) => {
            validatorsAdapter.removeOne(state.validators, name);
        },
        validatorStateToggled: (state, { payload: name }: PayloadAction<CollectionName>) => {
            const disabled = validatorsSelectors.selectById(state.validators, name)?.Disabled;
            if (typeof disabled === "boolean") {
                validatorsAdapter.updateOne(state.validators, {
                    id: name,
                    changes: {
                        Disabled: !disabled,
                    },
                });
            }
        },
        allSelectedCollectionNamesToggled: (state) => {
            if (state.selectedCollectionNames.length === 0) {
                state.selectedCollectionNames = validatorsSelectors.selectIds(state.validators) as CollectionName[];
            } else {
                state.selectedCollectionNames = [];
            }
        },
        selectedCollectionNameToggled: (state, { payload: name }: PayloadAction<CollectionName>) => {
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
