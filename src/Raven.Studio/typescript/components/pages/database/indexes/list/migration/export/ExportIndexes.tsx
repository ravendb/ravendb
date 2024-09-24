import { yupResolver } from "@hookform/resolvers/yup";
import fileDownloader from "common/fileDownloader";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormRadioToggleWithIcon, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { IndexSharedInfo } from "components/models/indexes";
import ExportIndexesList from "components/pages/database/indexes/list/migration/export/ExportIndexesList";
import { useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import { tryHandleSubmit } from "components/utils/common";
import IndexUtils from "components/utils/IndexUtils";
import moment from "moment";
import React, { useEffect, useMemo, useState } from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Button, Form, InputGroup, InputGroupText, Modal, ModalBody, ModalFooter } from "reactstrap";
import * as yup from "yup";
import RichAlert from "components/common/RichAlert";

type ExportMode = "database" | "file";

interface ExportIndexesProps {
    indexes?: IndexSharedInfo[];
    selectedNames: string[];
    toggle: () => void;
}

export function ExportIndexes(props: ExportIndexesProps) {
    const { toggle, indexes, selectedNames } = props;

    const getHasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess);

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const availableDatabaseNames = useAppSelector(databaseSelectors.allDatabases)
        .filter((x) => x.name !== activeDatabaseName && !x.isDisabled && getHasDatabaseWriteAccess(x.name))
        .map((x) => x.name)
        .sort();

    const databaseOptions: SelectOption[] = availableDatabaseNames.map((x) => ({ label: x, value: x }));

    const { control, formState, handleSubmit, setValue } = useForm<FormData>({
        defaultValues: {
            databaseName: "",
            exportMode: availableDatabaseNames.length > 0 ? "database" : "file",
        },
        resolver: yupResolver(schema),
    });

    const { exportMode, databaseName } = useWatch({ control });

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)(databaseName);
    const hasDatabaseWriteAccess = getHasDatabaseWriteAccess(databaseName);

    const [disabledReason, setDisabledReason] = useState<string>();

    const filteredIndexes = useMemo(() => {
        const selectedIndexes = indexes.filter((x) => selectedNames.includes(x.name));
        const isAnyAutoIndexSelected = selectedIndexes.some((x) => IndexUtils.isAutoIndex(x));
        const selectedStaticIndexes = selectedIndexes.filter((x) => IndexUtils.isStaticIndex(x));

        let availableIndexes = selectedStaticIndexes;
        let unavailableIndexes: IndexSharedInfo[] = [];

        if (exportMode === "database" && !!databaseName && !hasDatabaseAdminAccess) {
            availableIndexes = selectedStaticIndexes.filter((x) => !IndexUtils.isCsharpIndex(x));
            unavailableIndexes = selectedStaticIndexes.filter((x) => IndexUtils.isCsharpIndex(x));
            setDisabledReason("Creating a C# index requires database administrator access");
        }

        return {
            availableIndexes,
            unavailableIndexes,
            isAnyAutoIndexSelected,
        };
    }, [databaseName, exportMode, hasDatabaseAdminAccess, indexes, selectedNames]);

    // Clear state on mode change
    useEffect(() => {
        setValue("databaseName", "");
    }, [exportMode, setValue]);

    const { indexesService } = useServices();

    const handleExport: SubmitHandler<FormData> = async ({ databaseName, exportMode }) => {
        return tryHandleSubmit(async () => {
            const indexNames = filteredIndexes.availableIndexes.map((x) => x.name);
            if (indexNames.length === 0) {
                toggle();
                return;
            }

            const indexDefinitions = await indexesService.getDefinitions(activeDatabaseName, { indexNames });

            switch (exportMode) {
                case "database": {
                    const csharpIndexes = indexDefinitions.filter((x) => IndexUtils.isCsharpIndex(x.Type));
                    const jsIndexes = indexDefinitions.filter((x) => IndexUtils.isJavaScriptIndex(x.Type));

                    if (hasDatabaseAdminAccess && csharpIndexes.length > 0) {
                        await indexesService.saveDefinitions(csharpIndexes, false, databaseName);
                    }
                    if (hasDatabaseWriteAccess && jsIndexes.length > 0) {
                        await indexesService.saveDefinitions(jsIndexes, true, databaseName);
                    }
                    break;
                }
                case "file": {
                    const fileName = `Indexes-of-${activeDatabaseName}-${moment().format("YYYY-MM-DD_HH-mm-ss")}.json`;
                    fileDownloader.downloadAsJson({ Indexes: indexDefinitions }, fileName);
                    break;
                }
                default:
                    assertUnreachable(exportMode);
            }

            toggle();
        });
    };

    return (
        <Modal
            isOpen
            toggle={toggle}
            size="lg"
            wrapClassName="bs5"
            contentClassName={`modal-border bulge-primary`}
            centered
        >
            <Form control={control} onSubmit={handleSubmit(handleExport)}>
                <ModalBody className="vstack gap-4 position-relative">
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={toggle} />
                    </div>

                    <Icon icon="index-import" color="primary" className="text-center fs-1" margin="m-0" />
                    <div className="lead text-center">
                        You&apos;re about to <span className="fw-bold">export</span> selected{" "}
                        <span className="fw-bold">({filteredIndexes.availableIndexes.length})</span>{" "}
                        {pluralizeHelpers.pluralize(filteredIndexes.availableIndexes.length, "index", "indexes", true)}
                    </div>
                    <div className="mx-auto">
                        <FormRadioToggleWithIcon
                            control={control}
                            name="exportMode"
                            leftItem={databaseRadioToggleItem}
                            rightItem={fileRadioToggleItem}
                        />
                    </div>
                    {exportMode === "database" && (
                        <InputGroup>
                            <InputGroupText>
                                <Icon icon="database" margin="m-0" />
                            </InputGroupText>
                            <FormSelect
                                control={control}
                                name="databaseName"
                                placeholder="Select destination database"
                                options={databaseOptions}
                            />
                        </InputGroup>
                    )}

                    <ExportIndexesList
                        availableIndexes={filteredIndexes.availableIndexes}
                        unavailableIndexes={filteredIndexes.unavailableIndexes}
                        disabledReason={disabledReason}
                    />

                    <div className="vstack gap-2">
                        {filteredIndexes.isAnyAutoIndexSelected && (
                            <RichAlert variant="info">All selected Auto-indexes are skipped</RichAlert>
                        )}
                        {exportMode === "database" && (
                            <RichAlert variant="info">
                                All conflicting indexes in the destination database will be overwritten after the export
                                is completed
                            </RichAlert>
                        )}
                    </div>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" color="link" title="Cancel" className="link-muted" onClick={toggle}>
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        type="submit"
                        color="success"
                        title="Export indexes"
                        className="rounded-pill"
                        isSpinning={formState.isSubmitting}
                        icon="export"
                    >
                        Export indexes to a {exportMode}
                    </ButtonWithSpinner>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

const databaseRadioToggleItem: RadioToggleWithIconInputItem<ExportMode> = {
    label: "To a database",
    value: "database",
    iconName: "database",
};

const fileRadioToggleItem: RadioToggleWithIconInputItem<ExportMode> = {
    label: "To a file",
    value: "file",
    iconName: "document",
};

const schema = yup.object({
    exportMode: yup.string<ExportMode>().required(),
    databaseName: yup.string().when("exportMode", {
        is: "database",
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;
