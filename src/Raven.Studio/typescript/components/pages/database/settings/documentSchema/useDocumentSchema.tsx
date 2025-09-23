import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import { SelectOption } from "components/common/select/Select";
import { useEffect, useState } from "react";
import { useServices } from "hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import { DocumentSchemaFormData } from "components/pages/database/settings/documentSchema/DocumentSchema";

export const useDocumentSchema = () => {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const validators = useAppSelector(documentSchemaSelectors.allValidators);
    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const draftIds = useAppSelector(documentSchemaSelectors.newDraftIds);

    const options: SelectOption[] = allCollectionNames.map((x) => ({ label: x, value: x }));
    const [selectedCollections, setSelectedCollections] = useState<SelectOption[]>([]);

    const filteredValidators = selectedCollections.length
        ? validators.filter((v) => selectedCollections.some((o) => o.value === v.Name))
        : validators;

    const { databasesService } = useServices();

    const asyncLoadValidators = useAsync(async () => {
        const result = await databasesService.getSchemaValidation(databaseName);
        dispatch(documentSchemaActions.validatorsLoadedFromServer(result));
        return result;
    }, []);

    useEffect(() => {
        return () => {
            dispatch(documentSchemaActions.reset());
        };
    }, []);

    const handleAddNew = () => {
        dispatch(documentSchemaActions.draftAdded(undefined));
    };

    const handleOnSelectCollection = (collectionNames: SelectOption[]) => {
        setSelectedCollections(collectionNames);
    };

    const handleCancelNew = (id: string) => {
        dispatch(documentSchemaActions.draftRemoved(id));
    };

    const asyncSaveValidators = useAsyncCallback(async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    });

    const handleNewSchemaSubmit = async (id: string, data: DocumentSchemaFormData) => {
        const newItem = documentSchemaUtils.maptoDocumentSchemaValidatorConfigDto(data);
        dispatch(documentSchemaActions.validatorAdded(newItem));
        await asyncSaveValidators.execute([...validators.filter((v) => v.Name !== newItem.Name), newItem]);
        dispatch(documentSchemaActions.draftRemoved(id));
    };

    return {
        handleAddNew,
        selectedCollections,
        setSelectedCollections,
        asyncLoadValidators,
        asyncSaveValidators,
        filteredValidators,
        allCollectionNames,
        draftIds,
        options,
        handleOnSelectCollection,
        handleCancelNew,
        handleNewSchemaSubmit,
    };
};
