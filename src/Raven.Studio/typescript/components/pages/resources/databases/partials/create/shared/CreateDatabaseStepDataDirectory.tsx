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
import { Icon } from "components/common/Icon";

interface CreateDatabaseStepPathProps {
    manualSelectedNodes: string[];
    isBackupFolder: boolean;
}

export default function CreateDatabaseStepPath({ manualSelectedNodes, isBackupFolder }: CreateDatabaseStepPathProps) {
    const { control, trigger } = useFormContext<CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData>();
    const {
        basicInfoStep: { databaseName },
        dataDirectoryStep,
    } = useWatch({ control });
    const { resourcesService } = useServices();

    const allNodeTags = useAppSelector(clusterSelectors.allNodeTags);
    const selectedNodeTags = manualSelectedNodes ?? allNodeTags;

    const asyncGetFolderOptions = useAsyncDebounce(
        async (path, isBackupFolder) => {
            if (!path || !(await trigger("dataDirectoryStep.directory"))) {
                return [];
            }

            const dto = await resourcesService.getFolderPathOptions_ServerLocal(path, isBackupFolder);
            return dto?.List.map((x) => ({ value: x, label: x }));
        },
        [dataDirectoryStep.directory, isBackupFolder]
    );

    const asyncGetDatabaseLocation = useAsyncDebounce(
        async (databaseName, directory) => {
            if (!directory || !(await trigger("dataDirectoryStep.directory"))) {
                return {
                    List: [],
                };
            }

            return await resourcesService.getDatabaseLocation(databaseName, directory);
        },
        [databaseName, dataDirectoryStep.directory]
    );

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
            <div className="mt-2 well p-1 rounded-2 d-flex justify-content-center">
                <Spinner />
            </div>
        );
    }

    const filteredLocations = nodeTagsToDisplay
        ? asyncGetDatabaseLocation.result.List.filter((x) => nodeTagsToDisplay.includes(x.NodeTag))
        : asyncGetDatabaseLocation.result.List;

    return (
        <div className="well mt-2 p-1 rounded-2 vstack gap-1">
            {filteredLocations.map((location) => (
                <div
                    key={`${location.NodeTag}-${location.FullPath}`}
                    color="node"
                    className="hstack gap-4 align-items-start card p-2"
                >
                    <div className="text-center flex-shrink-0">
                        <div className="small-label mb-1">Node tag</div>
                        <strong className="fs-3 text-node">
                            <Icon icon="node" /> {location.NodeTag}
                        </strong>
                    </div>
                    <div className="vstack flex-grow gap-2">
                        <div>
                            <div className="small-label">
                                <Icon icon="path" />
                                Path
                            </div>
                            <code className="text-info word-break">{location.FullPath}</code>
                        </div>

                        {location.Error ? (
                            <Alert color="danger">{location.Error}</Alert>
                        ) : (
                            <>
                                {location.FreeSpaceHumane ? (
                                    <div className="hstack gap-3">
                                        <div>
                                            <div className="small-label">
                                                <Icon icon="storage" /> Free space
                                            </div>
                                            <strong>{location.FreeSpaceHumane}</strong>
                                        </div>

                                        {location.TotalSpaceHumane && (
                                            <div>
                                                <div className="small-label">
                                                    <Icon icon="storage" /> Total space
                                                </div>
                                                <strong>{location.TotalSpaceHumane}</strong>
                                            </div>
                                        )}
                                    </div>
                                ) : (
                                    <Alert color="warning">Path is unreachable</Alert>
                                )}
                            </>
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}
