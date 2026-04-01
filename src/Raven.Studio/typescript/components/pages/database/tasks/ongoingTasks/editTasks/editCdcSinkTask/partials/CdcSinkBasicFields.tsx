import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { sortBy } from "lodash";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import { CdcSinkFormData } from "../types";
import CdcSinkService from "../services/cdcSinkService";
import InputGroup from "react-bootstrap/InputGroup";
import { Icon } from "components/common/Icon";
import { useState } from "react";

export default function CdcSinkBasicFields() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const nodes = useAppSelector(clusterSelectors.allNodes);

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const { tasksService } = useServices();
    const { control, setValue } = useFormContext<CdcSinkFormData>();
    const formValues = useWatch({ control });

    const [verifyResult, setVerifyResult] =
        useState<Raven.Server.Documents.CdcSink.CdcSinkVerificationResult | null>(null);

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);

        const connectionStrings = Object.keys(result.SqlConnectionStrings ?? {});

        return sortBy(connectionStrings, (x) => x.toUpperCase()).map(
            (x) => ({ value: x, label: x }) satisfies SelectOption
        );
    }, []);

    const handleConnectionStringSave = async (connectionName: string) => {
        await asyncGetConnectionStringsOptions.execute();
        setValue("connectionStringName", connectionName, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
        });
        toggleIsNewConnectionStringOpen();
    };

    const asyncVerifySource = useAsyncCallback(async () => {
        if (!formValues.connectionStringName) {
            return;
        }
        const result = await CdcSinkService.verify(databaseName, formValues.connectionStringName);
        setVerifyResult(result);
    });

    const possibleMentorOptions: SelectOption[] = nodes
        .filter((x) => x.type === "Member")
        .map((x) => ({ value: x.nodeTag, label: `Node ${x.nodeTag}` }));

    return (
        <div>
            <h3>Basic Configuration</h3>

            <FormGroup>
                <FormLabel>Task Name</FormLabel>
                <FormInput type="text" control={control} name="name" placeholder="CDC Sink task name" />
            </FormGroup>

            <FormGroup>
                <FormSwitch control={control} name="disabled">
                    Disable Task
                </FormSwitch>
            </FormGroup>

            <FormGroup>
                <FormLabel>Connection String</FormLabel>
                <InputGroup>
                    <FormSelect
                        control={control}
                        name="connectionStringName"
                        options={asyncGetConnectionStringsOptions.result ?? []}
                        isLoading={asyncGetConnectionStringsOptions.loading}
                    />
                    <InputGroup.Text>
                        <ButtonWithSpinner
                            variant="link"
                            className="text-reset px-0"
                            icon="plus"
                            isSpinning={asyncGetConnectionStringsOptions.loading}
                            onClick={toggleIsNewConnectionStringOpen}
                        >
                            Create new SQL connection string
                        </ButtonWithSpinner>
                    </InputGroup.Text>
                    {isNewConnectionStringOpen && (
                        <EditConnectionStrings
                            initialConnection={{ type: "Sql" }}
                            afterSave={handleConnectionStringSave}
                            afterClose={toggleIsNewConnectionStringOpen}
                        />
                    )}
                </InputGroup>
            </FormGroup>

            <FormGroup>
                <FormSwitch control={control} name="isSetResponsibleNode">
                    Set Responsible Node
                </FormSwitch>
            </FormGroup>

            {formValues.isSetResponsibleNode && (
                <FormGroup>
                    <FormLabel>Responsible Node</FormLabel>
                    {possibleMentorOptions.length === 0 ? (
                        <RichAlert variant="warning">
                            No nodes are currently available for selection.
                        </RichAlert>
                    ) : (
                        <FormSelect control={control} name="responsibleNode" options={possibleMentorOptions} />
                    )}
                </FormGroup>
            )}

            <FormGroup>
                <ButtonWithSpinner
                    variant="secondary"
                    icon="check"
                    isSpinning={asyncVerifySource.loading}
                    onClick={asyncVerifySource.execute}
                    disabled={!formValues.connectionStringName}
                >
                    Verify Source
                </ButtonWithSpinner>

                {verifyResult && (
                    <div className="mt-2">
                        {verifyResult.Success ? (
                            <RichAlert variant="success">
                                <Icon icon="check" /> Connection verified successfully.
                                {!verifyResult.HasPermissionToSetup && (
                                    <div className="mt-1 text-warning">
                                        <Icon icon="warning" /> User does not have permission to setup CDC.
                                    </div>
                                )}
                            </RichAlert>
                        ) : (
                            <RichAlert variant="danger">
                                <Icon icon="cancel" /> Verification failed.
                                {verifyResult.Errors?.map((error, i) => (
                                    <div key={i} className="mt-1">{error}</div>
                                ))}
                            </RichAlert>
                        )}
                        {verifyResult.Warnings?.length > 0 && (
                            <RichAlert variant="warning" className="mt-1">
                                {verifyResult.Warnings.map((warning, i) => (
                                    <div key={i}>{warning}</div>
                                ))}
                            </RichAlert>
                        )}
                    </div>
                )}
            </FormGroup>
        </div>
    );
}
