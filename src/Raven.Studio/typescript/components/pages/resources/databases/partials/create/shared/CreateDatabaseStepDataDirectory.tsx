import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Alert, InputGroup, InputGroupText, Spinner } from "reactstrap";
import { FormCheckbox, FormSelectAutocomplete } from "components/common/Form";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/utils/hooks/useAsyncDebounce";
import { UseAsyncReturn } from "react-async-hook";
import { CreateDatabaseFromBackupFormData } from "components/pages/resources/databases/partials/create/formBackup/createDatabaseFromBackupValidation";
import { CreateDatabaseRegularFormData } from "components/pages/resources/databases/partials/create/regular/createDatabaseRegularValidation";

interface CreateDatabaseStepPathProps {
    manualSelectedNodes: string[];
    isBackupFolder: boolean;
}

export default function CreateDatabaseStepPath({ manualSelectedNodes, isBackupFolder }: CreateDatabaseStepPathProps) {
    const { control } = useFormContext<CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData>();
    const {
        basicInfoStep: { databaseName },
        dataDirectoryStep,
    } = useWatch({ control });
    const { resourcesService } = useServices();

    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const selectedNodeTags = manualSelectedNodes ?? allNodeTags;

    const asyncGetFolderOptions = useAsyncDebounce(
        async (path, isBackupFolder) => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(path, isBackupFolder);
            return dto?.List.map((x) => ({ value: x, label: x }));
        },
        [dataDirectoryStep.directory, isBackupFolder]
    );

    const asyncGetDatabaseLocation = useAsyncDebounce(resourcesService.getDatabaseLocation, [
        databaseName,
        dataDirectoryStep.directory,
    ]);

    return (
        <div>
            <h2 className="text-center">Data Directory</h2>
            <InputGroup className="my-4">
                <InputGroupText>
                    <FormCheckbox control={control} name="dataDirectoryStep.isDefault">
                        Use server directory
                    </FormCheckbox>
                </InputGroupText>
                <FormSelectAutocomplete
                    control={control}
                    name="dataDirectoryStep.directory"
                    placeholder={dataDirectoryStep.isDefault ? "" : "Enter database directory"}
                    options={asyncGetFolderOptions.result ?? []}
                    isLoading={asyncGetFolderOptions.loading}
                    isDisabled={dataDirectoryStep.isDefault}
                />
            </InputGroup>
            <PathInfo asyncGetDatabaseLocation={asyncGetDatabaseLocation} nodeTagsToDisplay={selectedNodeTags} />
        </div>
    );
}

function PathInfo({
    asyncGetDatabaseLocation,
    nodeTagsToDisplay,
}: {
    asyncGetDatabaseLocation: UseAsyncReturn<Raven.Server.Web.Studio.DataDirectoryResult, (string | boolean)[]>;
    nodeTagsToDisplay?: string[];
}) {
    if (
        asyncGetDatabaseLocation.status === "not-requested" ||
        asyncGetDatabaseLocation.status === "error" ||
        asyncGetDatabaseLocation.result?.List?.length === 0
    ) {
        return null;
    }

    if (asyncGetDatabaseLocation.status === "loading") {
        return (
            <Alert color="info" className="mt-2">
                <Spinner />
            </Alert>
        );
    }

    const filteredLocations = nodeTagsToDisplay
        ? asyncGetDatabaseLocation.result.List.filter((x) => nodeTagsToDisplay.includes(x.NodeTag))
        : asyncGetDatabaseLocation.result.List;

    return (
        <Alert color="info" className="mt-2">
            {filteredLocations.map((location) => (
                <div key={`${location.NodeTag}-${location.FullPath}`}>
                    <small>
                        <span>
                            Node tag: <strong>{location.NodeTag}</strong>
                        </span>
                        <br />
                        <span>
                            Path: <strong>{location.FullPath}</strong>
                        </span>
                        <br />
                        {location.Error ? (
                            <strong>{location.Error}</strong>
                        ) : (
                            <>
                                {location.FreeSpaceHumane ? (
                                    <span>
                                        Free space: <strong>{location.FreeSpaceHumane}</strong>{" "}
                                        {location.TotalSpaceHumane && (
                                            <span>
                                                {"(Total: "}
                                                <strong>{location.TotalSpaceHumane}</strong>
                                                {")"}
                                            </span>
                                        )}
                                    </span>
                                ) : (
                                    <strong>(Path is unreachable)</strong>
                                )}
                            </>
                        )}
                    </small>
                </div>
            ))}
        </Alert>
    );
}
