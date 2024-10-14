import { yupResolver } from "@hookform/resolvers/yup";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { EmptySet } from "components/common/EmptySet";
import FileDropzone from "components/common/FileDropzone";
import { FormRadioToggleWithIcon, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useCheckboxes } from "components/hooks/useCheckboxes";
import { useServices } from "components/hooks/useServices";
import ImportIndexesList from "components/pages/database/indexes/list/migration/import/ImportIndexesList";
import { useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import IndexUtils from "components/utils/IndexUtils";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useAsync } from "react-async-hook";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Button, Form, InputGroup, InputGroupText, Modal, ModalBody, ModalFooter } from "reactstrap";
import * as yup from "yup";
import RichAlert from "components/common/RichAlert";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;

type ImportMode = "database" | "file";

interface ImportIndexesProps {
    toggle: () => void;
}

export function ImportIndexes(props: ImportIndexesProps) {
    const { toggle } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const availableDatabaseNames = useAppSelector(databaseSelectors.allDatabases)
        .filter((x) => x.name !== activeDatabaseName && !x.isDisabled)
        .map((x) => x.name)
        .sort();

    const databaseOptions: SelectOption[] = availableDatabaseNames.map((x) => ({ label: x, value: x }));

    const { control, formState, handleSubmit, setValue } = useForm<FormData>({
        mode: "all",
        defaultValues: {
            selectedDatabaseName: "",
            importMode: availableDatabaseNames.length > 0 ? "database" : "file",
            selectedIndexNames: [],
        },
        resolver: yupResolver(formSchema),
    });
    const { importMode, selectedDatabaseName, selectedIndexNames } = useWatch({ control });

    const [disabledReason, setDisabledReason] = useState<string>();

    const [indexDefinitions, setIndexDefinitions] = useState<IndexDefinition[]>([]);
    const filteredIndexes = useMemo(() => {
        const sortedIndexes = [...indexDefinitions].sort((a, b) => a.Name.localeCompare(b.Name));

        const isAnyAutoIndex = sortedIndexes.some((x) => IndexUtils.isAutoIndex(x));
        const staticIndexes = sortedIndexes.filter((x) => IndexUtils.isStaticIndex(x));

        let availableIndexes = staticIndexes;
        let unavailableIndexes: IndexDefinition[] = [];

        if (!hasDatabaseAdminAccess) {
            availableIndexes = staticIndexes.filter((x) => !IndexUtils.isCsharpIndex(x));
            unavailableIndexes = staticIndexes.filter((x) => IndexUtils.isCsharpIndex(x));
            setDisabledReason("Creating a C# index requires database administrator access");
        }

        setValue(
            "selectedIndexNames",
            availableIndexes.map((x) => x.Name)
        );

        return {
            availableIndexes,
            unavailableIndexes,
            isAnyAutoIndex,
        };
    }, [hasDatabaseAdminAccess, indexDefinitions, setValue]);

    const clearIndexes = useCallback(() => {
        setIndexDefinitions([]);
        setValue("selectedIndexNames", []);
    }, [setValue, setIndexDefinitions]);

    // Clear states on mode change
    useEffect(() => {
        clearIndexes();
        setValue("selectedDatabaseName", "");
    }, [importMode, clearIndexes, setValue]);

    const {
        selectionState,
        toggleOne: toggleIndexName,
        toggleAll: toggleAllIndexNames,
    } = useCheckboxes({
        allItems: filteredIndexes.availableIndexes.map((x) => x.Name),
        selectedItems: selectedIndexNames,
        setValue: (x) => setValue("selectedIndexNames", x),
    });

    const { indexesService } = useServices();

    // Handle fetching index definitions from selected database
    useAsync(
        async () => {
            if (!selectedDatabaseName) {
                return;
            }

            const result = await indexesService.getDefinitions(selectedDatabaseName);
            const staticIndexes = result.filter((x) => !IndexUtils.isAutoIndex(x));
            setIndexDefinitions(staticIndexes);
        },
        [selectedDatabaseName],
        {
            onError: () => {
                clearIndexes();
            },
        }
    );

    const handleFileChange = (files: File[]) => {
        const file = files[0];
        if (!file) {
            clearIndexes();
            return;
        }

        const reader = new FileReader();

        reader.onload = function () {
            const textResult = reader.result as string; // it is a string, because we call readAsText()

            try {
                const fileContent = indexDefinitionsSchema.validateSync(JSON.parse(textResult));
                setIndexDefinitions(fileContent.Indexes as IndexDefinition[]);
            } catch (e) {
                clearIndexes();
                messagePublisher.reportError("Failed to load file", e.message);
            }
        };

        reader.onerror = function () {
            clearIndexes();
            messagePublisher.reportError("Failed to load file", reader.error.message);
        };

        reader.readAsText(file);
    };

    const handleImport: SubmitHandler<FormData> = async () => {
        return tryHandleSubmit(async () => {
            const selectedIndexDefinitions = indexDefinitions.filter((x) => selectedIndexNames.includes(x.Name));

            const csharpIndexes = selectedIndexDefinitions.filter((x) => IndexUtils.isCsharpIndex(x.Type));
            const jsIndexes = selectedIndexDefinitions.filter((x) => IndexUtils.isJavaScriptIndex(x.Type));

            if (hasDatabaseAdminAccess && csharpIndexes.length > 0) {
                await indexesService.saveDefinitions(csharpIndexes, false, activeDatabaseName);
            }
            if (hasDatabaseWriteAccess && jsIndexes.length > 0) {
                await indexesService.saveDefinitions(jsIndexes, true, activeDatabaseName);
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
            <Form control={control} onSubmit={handleSubmit(handleImport)}>
                <ModalBody className="vstack gap-4 position-relative">
                    <Icon icon="index-import" color="primary" className="text-center fs-1" margin="m-0" />
                    <div className="lead text-center">
                        You&apos;re about to <span className="fw-bold">import</span> indexes
                    </div>
                    <div className="mx-auto">
                        <FormRadioToggleWithIcon
                            control={control}
                            name="importMode"
                            leftItem={databaseRadioToggleItem}
                            rightItem={fileRadioToggleItem}
                        />
                    </div>

                    {importMode === "database" && (
                        <InputGroup>
                            <InputGroupText>
                                <Icon icon="database" margin="m-0" />
                            </InputGroupText>
                            <FormSelect
                                control={control}
                                name="selectedDatabaseName"
                                placeholder="Select origin database"
                                options={databaseOptions}
                            />
                        </InputGroup>
                    )}

                    {importMode === "file" && (
                        <FileDropzone onChange={handleFileChange} validExtensions={["json"]} maxFiles={1} />
                    )}

                    <ImportIndexesList
                        availableIndexes={filteredIndexes.availableIndexes}
                        unavailableIndexes={filteredIndexes.unavailableIndexes}
                        disabledReason={disabledReason}
                        selectionState={selectionState}
                        selectedIndexNames={selectedIndexNames}
                        toggleAllIndexNames={toggleAllIndexNames}
                        toggleIndexName={toggleIndexName}
                    />

                    {!indexDefinitions?.length && (
                        <EmptySet compact className="text-muted">
                            No indexes found.
                            <br />
                            You can select a database or a file with indexes.
                        </EmptySet>
                    )}

                    {formState.errors?.selectedIndexNames && (
                        <div className="badge bg-danger rounded-pill w-fit-content">
                            {formState.errors.selectedIndexNames.message}
                        </div>
                    )}

                    <div className="vstack gap-2">
                        {selectedDatabaseName && importMode === "database" && filteredIndexes.isAnyAutoIndex && (
                            <RichAlert variant="info">All Auto-indexes will be skipped</RichAlert>
                        )}
                        <RichAlert variant="info">
                            All conflicting indexes will be overwritten after the import is completed
                        </RichAlert>
                    </div>

                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={toggle} />
                    </div>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" color="link" className="link-muted" onClick={toggle}>
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        type="submit"
                        color="success"
                        title="Import indexes"
                        className="rounded-pill"
                        icon="import"
                        isSpinning={formState.isSubmitting}
                    >
                        Import indexes from a {importMode}
                    </ButtonWithSpinner>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

const databaseRadioToggleItem: RadioToggleWithIconInputItem<ImportMode> = {
    label: "From a database",
    value: "database",
    iconName: "database",
};

const fileRadioToggleItem: RadioToggleWithIconInputItem<ImportMode> = {
    label: "From a file",
    value: "file",
    iconName: "document",
};

const formSchema = yup.object({
    importMode: yup.string<ImportMode>().required(),
    selectedDatabaseName: yup.string().when("importMode", {
        is: "database",
        then: (schema) => schema.required(),
    }),
    selectedIndexNames: yup.array().of(yup.string().required()).min(1),
});

type FormData = yup.InferType<typeof formSchema>;

// It only checks the name because we need it to display a list
const indexDefinitionsSchema = yup.object({
    Indexes: yup.array().of(
        yup.object({
            Name: yup.string().required(),
        })
    ),
});
