import { yupResolver } from "@hookform/resolvers/yup";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Checkbox } from "components/common/Checkbox";
import CheckboxSelectAll from "components/common/CheckboxSelectAll";
import { EmptySet } from "components/common/EmptySet";
import FileDropzone from "components/common/FileDropzone";
import { FormRadioToggleWithIcon, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import { useCheckboxes } from "components/hooks/useCheckboxes";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import { tryHandleSubmit } from "components/utils/common";
import IndexUtils from "components/utils/IndexUtils";
import React, { useCallback, useEffect, useState } from "react";
import { useAsync } from "react-async-hook";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    Alert,
    Button,
    Form,
    InputGroup,
    InputGroupText,
    Label,
    ListGroup,
    ListGroupItem,
    Modal,
    ModalBody,
    ModalFooter,
} from "reactstrap";
import * as yup from "yup";

type IndexDefinition = Raven.Client.Documents.Indexes.IndexDefinition;

type ImportMode = "database" | "file";

interface ImportIndexesProps {
    toggle: () => void;
}

export function ImportIndexes(props: ImportIndexesProps) {
    const { toggle } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const availableDatabaseNames = useAppSelector(databaseSelectors.allDatabases)
        .filter((x) => x.name !== activeDatabaseName && !x.isDisabled)
        .map((x) => x.name)
        .sort();

    const { value: hasSomeAutoIndexes, setValue: setHasSomeAutoIndexes } = useBoolean(false);

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

    const [indexDefinitions, setIndexDefinitions] = useState<IndexDefinition[]>([]);

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
        allItems: indexDefinitions.map((x) => x.Name),
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

            setHasSomeAutoIndexes(staticIndexes.some((x) => IndexUtils.isAutoIndex(x)));
            setIndexDefinitions(staticIndexes);
            setValue(
                "selectedIndexNames",
                staticIndexes.map((x) => x.Name)
            );
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
                setValue(
                    "selectedIndexNames",
                    fileContent.Indexes.map((x) => x.Name)
                );
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
            await indexesService.saveDefinitions(activeDatabaseName, selectedIndexDefinitions);

            toggle();
        });
    };

    const getFormattedImportMode = () => {
        switch (importMode) {
            case "database":
                return "database";
            case "file":
                return "a file";
            default:
                assertUnreachable(importMode);
        }
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

                    {indexDefinitions.length > 0 && (
                        <div>
                            <CheckboxSelectAll
                                selectionState={selectionState}
                                toggleAll={toggleAllIndexNames}
                                allItemsCount={indexDefinitions.length}
                                selectedItemsCount={selectedIndexNames.length}
                            />
                            <div className="vstack gap-3 overflow-auto" style={{ maxHeight: "200px" }}>
                                <ListGroup>
                                    {indexDefinitions.map((definition) => (
                                        <ListGroupItem key={definition.Name}>
                                            <Label className="d-flex gap-1 align-items-center m-0 text-truncate">
                                                <Checkbox
                                                    toggleSelection={() => toggleIndexName(definition.Name)}
                                                    selected={selectedIndexNames.includes(definition.Name)}
                                                    size="md"
                                                    color="primary"
                                                />
                                                {definition.Name}
                                            </Label>
                                        </ListGroupItem>
                                    ))}
                                </ListGroup>
                            </div>
                        </div>
                    )}

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
                        {selectedDatabaseName && importMode === "database" && hasSomeAutoIndexes && (
                            <Alert color="info" className="text-left">
                                <Icon icon="info" />
                                All Auto-indexes will be skipped
                            </Alert>
                        )}
                        <Alert color="info" className="text-left">
                            <Icon icon="info" />
                            All the conflicting indexes will be overwritten after the import is done
                        </Alert>
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
                        Import indexes from {getFormattedImportMode()}
                    </ButtonWithSpinner>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

const databaseRadioToggleItem: RadioToggleWithIconInputItem<ImportMode> = {
    label: "From database",
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
