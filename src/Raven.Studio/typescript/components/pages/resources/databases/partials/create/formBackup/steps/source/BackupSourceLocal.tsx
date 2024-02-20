import { FormSelectCreatable } from "components/common/Form";
import { InputNotHidden, SelectOption } from "components/common/select/Select";
import { useServices } from "components/hooks/useServices";
import React from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { InputActionMeta } from "react-select";
import { Col, Row } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import CreateDatabaseFromBackupRestorePoint from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointField";
import { mapRestorePointDtoToSelectOptions } from "components/pages/resources/databases/partials/create/formBackup/steps/source/backupSourceUtils";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import EncryptionField from "components/pages/resources/databases/partials/create/formBackup/steps/source/EncryptionField";
import RestorePointsFields from "components/pages/resources/databases/partials/create/formBackup/steps/source/RestorePointsFields";

export default function BackupSourceLocal() {
    const { resourcesService } = useServices();
    const { control, setValue } = useFormContext<FormData>();

    const formValues = useWatch({
        control,
    });

    const directory = formValues.source.sourceData.local.directory;

    const asyncGetLocalFolderPathOptions = useAsyncDebounce(
        async (directory) => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(directory, true);

            return dto.List.map((x) => ({ value: x, label: x }) satisfies SelectOption);
        },
        [directory]
    );

    // TODO make autocomplete component?
    const onPathChange = (value: string, action: InputActionMeta) => {
        if (action?.action !== "input-blur" && action?.action !== "menu-close") {
            setValue("source.sourceData.local.directory", value);
        }
    };

    return (
        <>
            <Row className="mt-2">
                <Col lg="3">
                    <label className="col-form-label">Directory Path</label>
                </Col>
                <Col>
                    <FormSelectCreatable
                        control={control}
                        name="source.sourceData.local.directory"
                        options={asyncGetLocalFolderPathOptions.result || []}
                        isLoading={asyncGetLocalFolderPathOptions.loading}
                        inputValue={directory ?? ""}
                        placeholder="Enter backup directory path"
                        onInputChange={onPathChange}
                        components={{ Input: InputNotHidden }}
                        tabSelectsValue
                        blurInputOnSelect={false}
                    />
                </Col>
            </Row>
            <RestorePointsFields
                isSharded={formValues.basicInfo.isSharded}
                restorePointsFieldName="source.sourceData.googleCloud.restorePoints"
                mapRestorePoint={(field, index) => <SourceRestorePoint key={field.id} index={index} />}
            />
            <EncryptionField
                encryptionKeyFieldName="source.sourceData.local.encryptionKey"
                selectedSourceData={formValues.source.sourceData.local}
            />
        </>
    );
}

function SourceRestorePoint({ index }: { index: number }) {
    const { control } = useFormContext<FormData>();

    const formValues = useWatch({
        control,
    });

    const { remove } = useFieldArray({
        control,
        name: "source.sourceData.local.restorePoints",
    });

    const directory = formValues.source.sourceData.local.directory;
    const isSharded = formValues.basicInfo.isSharded;

    const { resourcesService } = useServices();

    const asyncGetRestorePointsOptions = useAsyncDebounce(
        async (directory, isSharded) => {
            const dto = await resourcesService.getRestorePoints_Local(
                _.trim(directory),
                null,
                true,
                isSharded ? index : undefined
            );
            return mapRestorePointDtoToSelectOptions(dto);
        },
        [directory, isSharded]
    );

    return (
        <CreateDatabaseFromBackupRestorePoint
            fieldName="source.sourceData.local.restorePoints"
            index={index}
            remove={remove}
            restorePointsOptions={asyncGetRestorePointsOptions.result ?? []}
            isLoading={asyncGetRestorePointsOptions.loading}
        />
    );
}
