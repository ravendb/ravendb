import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import { SelectOption } from "components/common/select/Select";
import { useEffect, useState } from "react";
import { useServices } from "hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import { DocumentSchemaFormData } from "components/pages/database/settings/documentSchema/DocumentSchema";
import { tryHandleSubmit } from "components/utils/common";
import { DocumentSchemaStatus } from "components/pages/database/settings/documentSchema/DocumentSchemaFilter";

export const useDocumentSchema = () => {
    const { databasesService } = useServices();
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const validators = useAppSelector(documentSchemaSelectors.allValidators);
    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const draftIds = useAppSelector(documentSchemaSelectors.newDraftIds);
    const isGlobalDisabled = useAppSelector(documentSchemaSelectors.isGlobalDisabled);
    const [selectedCollections, setSelectedCollections] = useState<SelectOption[]>([]);
    const [selectedStatuses, setSelectedStatuses] = useState<DocumentSchemaStatus[]>([]);

    const options: SelectOption[] = allCollectionNames.map((x) => ({ label: x, value: x }));

    const filteredValidators = validators.filter((v) => {
        const matchesCollection =
            selectedCollections.length === 0 || selectedCollections.some((o) => o.value === v.Name);
        const matchesStatus =
            selectedStatuses.length === 0 ||
            selectedStatuses.length === 2 ||
            (selectedStatuses.includes("enabled") && !v.Disabled) ||
            (selectedStatuses.includes("disabled") && v.Disabled);
        return matchesCollection && matchesStatus;
    });

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
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items, isGlobalDisabled)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    });

    const handleNewSchemaSubmit = (id: string, data: DocumentSchemaFormData) => {
        return tryHandleSubmit(async () => {
            const newItem = documentSchemaUtils.mapToDocumentSchemaValidatorConfigDto(data);
            await asyncSaveValidators.execute([...validators.filter((v) => v.Name !== newItem.Name), newItem]);
            dispatch(documentSchemaActions.draftRemoved(id));
            dispatch(documentSchemaActions.validatorAdded(newItem));
        });
    };

    const hasAnyValidator = validators.length > 0;

    return {
        handleAddNew,
        selectedCollections,
        filteredValidators,
        allCollectionNames,
        draftIds,
        options,
        handleOnSelectCollection,
        handleCancelNew,
        handleNewSchemaSubmit,
        hasAnyValidator,
        selectedStatuses,
        setSelectedStatuses,
    };
};
