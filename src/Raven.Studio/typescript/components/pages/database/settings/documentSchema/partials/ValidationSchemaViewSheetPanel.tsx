import { ViewSheet } from "components/common/splitView/ViewSheet";
import { Icon } from "components/common/Icon";
import React, { ChangeEvent, useMemo } from "react";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import * as yup from "yup";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import classNames from "classnames";
import { useAsyncCallback } from "react-async-hook";
import { DocumentSchemaValidatorConfig } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import notificationCenter from "common/notifications/notificationCenter";
import Accordion from "react-bootstrap/Accordion";
import "./ValidationSchemaViewSheetPanel.scss";
import Spinner from "react-bootstrap/Spinner";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import SizeGetter from "components/common/SizeGetter";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import ProgressBar from "react-bootstrap/ProgressBar";
import { isEmpty } from "common/typeUtils";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import Code from "components/common/Code";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import { Checkbox } from "components/common/Checkbox";

interface ValidationSchemaViewSheetPanelProps {
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[];
    isPlayground?: boolean;
}

interface ValidationOperationProgress extends ValidateSchemaResult {
    status?: "complete" | "loading" | "error";
    error?: unknown;
}

export function ValidationSchemaViewSheetPanel({ validators, isPlayground }: ValidationSchemaViewSheetPanelProps) {
    // TODO: At the moment, when i close viewSheet, the entire state is deleted. I need to add the ability to persist this state if, for example, someone runs a test.
    // In Phase 2 i will rewrite monitorOperationProgress logic into redux.
    const [monitorOperationProgress, setMonitorOperationProgress] = React.useState<
        Record<string, ValidationOperationProgress>
    >({});
    const [selectedCollections, setSelectedCollections] = React.useState<Record<string, boolean>>(() => {
        const initialState: Record<string, boolean> = {};
        validators.forEach((validator) => {
            initialState[validator.Name] = true;
        });
        return initialState;
    });
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const collections = useAppSelector(collectionsTrackerSelectors.collections);

    const form = useForm<ValidateSchemaFormData>({
        resolver: yupResolver(schema),
    });

    const activeValidators = useMemo(
        () => (isPlayground ? getSelectedValidators(validators, selectedCollections) : validators),
        [isPlayground, validators, selectedCollections]
    );

    const asyncGetOperationIds = useAsyncCallback(async (formData: ValidateSchemaFormData) => {
        const dtos: ValidateSchemaRequestDto[] = activeValidators.map((validator) =>
            documentSchemaUtils.mapToValidateSchemaRequestDto(validator, formData)
        );

        return await Promise.allSettled(
            dtos.map(async (dto) => await databasesService.validateSchema(databaseName, dto))
        );
    });

    const asyncTestSchema = useAsyncCallback(async (formData: ValidateSchemaFormData) => {
        if (activeValidators.length === 0) {
            return;
        }

        setMonitorOperationProgress({});

        const dtos: ValidateSchemaRequestDto[] = activeValidators.map((validator) =>
            documentSchemaUtils.mapToValidateSchemaRequestDto(validator, formData)
        );

        const operationResults = await asyncGetOperationIds.execute(formData);

        operationResults.forEach((result, idx) => {
            const collectionName = dtos[idx].Collection;

            setMonitorOperationProgress((prev) => ({
                ...prev,
                [collectionName]: {
                    Errors: {},
                    LastEtag: 0,
                    ErrorCount: 0,
                    ValidatedCount: 0,
                    status: "loading",
                },
            }));

            if (result.status === "rejected") {
                console.error(`Validation operation failed to start for ${collectionName}:`, result.reason);

                setMonitorOperationProgress((prev) => ({
                    ...prev,
                    [collectionName]: {
                        ...prev[collectionName],
                        status: "error",
                        error: result.reason,
                    },
                }));

                return;
            }

            const operationId = result.value.OperationId;

            notificationCenter.instance
                .monitorOperation<ValidationOperationProgress>(databaseName, operationId, (progress) =>
                    setMonitorOperationProgress((prev) => ({
                        ...prev,
                        [collectionName]: { ...progress, status: "loading" },
                    }))
                )
                .then((finalResult) => {
                    setMonitorOperationProgress((prev) => ({
                        ...prev,
                        [collectionName]: { ...finalResult, status: "complete" },
                    }));
                })
                .catch((error) => {
                    console.error(`Validation failed for ${collectionName}:`, error);
                    setMonitorOperationProgress((prev) => ({
                        ...prev,
                        [collectionName]: { ...prev[collectionName], status: "error", error },
                    }));
                });
        });
    });

    const killOperation = useAsyncCallback(async () => {
        if (!asyncGetOperationIds.result) {
            return [];
        }

        const settledResults = asyncGetOperationIds.result;

        return Promise.allSettled(
            settledResults
                .filter((r) => r.status === "fulfilled")
                .map((fulfilledResult) =>
                    databasesService.killOperation(databaseName, fulfilledResult.value.OperationId)
                )
        );
    });

    const { control, handleSubmit } = form;

    const formValues = useWatch({
        control,
    });

    const isValidating = validators.some((validator) => monitorOperationProgress[validator.Name]?.status === "loading");

    const isTestSettingsDisabled = !formValues.isTestSettingsEnabled || isValidating;

    const isDisabledRunTest = isPlayground && isEmpty(activeValidators);

    const handleSelectCollections = (validator: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">) => {
        setSelectedCollections((prev) => ({
            ...prev,
            [validator.Name]: !getIsCollectionSelected(validator.Name, prev),
        }));
    };

    return (
        <FormProvider {...form}>
            <form className="h-100" onSubmit={handleSubmit(asyncTestSchema.execute)}>
                <ViewSheet className="h-100 validation-schema-view-sheet-panel">
                    <ViewSheet.Header>
                        <div className="d-flex gap-2 align-items-center">
                            <h3 className="mb-0">
                                <Icon icon="rocket" className="text-primary" />
                                Validation schema test
                            </h3>
                        </div>
                    </ViewSheet.Header>
                    <ViewSheet.Body className="p-4">
                        <h4 className="w-100 text-center">
                            {isValidating ? (
                                <ValidationProgressSummary
                                    validators={activeValidators}
                                    monitorOperationProgress={monitorOperationProgress}
                                    collections={collections}
                                    isTestSettingsEnabled={formValues.isTestSettingsEnabled}
                                    maxDocumentsToValidate={formValues.maxDocumentsToValidate}
                                />
                            ) : (
                                <ValidationStatusMessage
                                    validators={validators}
                                    monitorOperationProgress={monitorOperationProgress}
                                />
                            )}
                        </h4>
                        <Accordion alwaysOpen className="mt-1  panel-bg-2" defaultActiveKey={[]}>
                            {validators.map((validator) => (
                                <ValidationCollectionAccordionItem
                                    key={validator.Name}
                                    validator={validator}
                                    monitorOperationProgress={monitorOperationProgress?.[validator.Name]}
                                    collections={collections}
                                    isTestSettingsEnabled={formValues.isTestSettingsEnabled}
                                    maxDocumentsToValidate={formValues.maxDocumentsToValidate}
                                    selected={getIsCollectionSelected(validator.Name, selectedCollections)}
                                    toggleSelection={() => handleSelectCollections(validator)}
                                    showSelection={isPlayground}
                                />
                            ))}
                        </Accordion>
                        <div
                            className={classNames("mt-4", {
                                "item-disabled": isValidating,
                            })}
                        >
                            <FormSwitch color="primary" control={control} name="isTestSettingsEnabled">
                                <h4 className="mb-0">Test settings</h4>
                            </FormSwitch>
                            <div>Specify maximum documents and run time - leave unset for unlimited.</div>
                            <div
                                className={classNames("mt-2", {
                                    "item-disabled": isTestSettingsDisabled,
                                })}
                            >
                                <FormGroup>
                                    <FormLabel>Maximum documents to scan (per collection)</FormLabel>
                                    <FormInput
                                        name="maxDocumentsToValidate"
                                        control={control}
                                        disabled={isTestSettingsDisabled}
                                        addon="documents"
                                        placeholder="e.g. 1000 (default: unlimited)"
                                        type="text"
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>Maximum error messages to return (per collection)</FormLabel>
                                    <FormInput
                                        name="maxErrorMessages"
                                        control={control}
                                        disabled={isTestSettingsDisabled}
                                        placeholder="e.g. 1000 (default: unlimited)"
                                        addon="errors"
                                        type="text"
                                    />
                                </FormGroup>
                            </div>
                        </div>
                    </ViewSheet.Body>
                    <ViewSheet.Footer className="d-flex justify-content-end">
                        {isValidating ? (
                            <ButtonWithSpinner
                                isSpinning={killOperation.loading}
                                icon="stop"
                                variant="danger"
                                className="rounded-pill"
                                onClick={killOperation.execute}
                            >
                                Abort operation
                            </ButtonWithSpinner>
                        ) : (
                            <ButtonWithSpinner
                                isSpinning={isValidating}
                                icon="start"
                                variant="primary"
                                type="submit"
                                disabled={isDisabledRunTest}
                                className="rounded-pill"
                            >
                                Run test
                            </ButtonWithSpinner>
                        )}
                    </ViewSheet.Footer>
                </ViewSheet>
            </form>
        </FormProvider>
    );
}

interface ValidatedDocumentsTableProps {
    width: number;
    loading: boolean;
    result?: ValidateSchemaResult;
}

interface TableProps {
    documentId: string;
    error: string;
}

const ValidatedDocumentsTable = ({ width, loading, result }: ValidatedDocumentsTableProps) => {
    const { columns } = useValidationInvalidDocumentsColumns(width);

    const data: TableProps[] = useMemo(() => {
        if (!result?.Errors) {
            return [];
        }

        return Object.entries(result.Errors).map(([key, value]) => ({
            documentId: key,
            error: value,
        }));
    }, [result]);

    const table = useReactTable({
        data,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });
    return (
        <VirtualTable
            className="panel-bg-1"
            isLoading={loading}
            table={table}
            heightInPx={virtualTableUtils.getHeightInPx(data.length, virtualTableConstants.defaultTableHeightInPx)}
        />
    );
};

function useValidationInvalidDocumentsColumns(availableWidth: number): { columns: ColumnDef<TableProps>[] } {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = React.useCallback(
        (percentage: number) => virtualTableUtils.getCellSizeProvider(bodyWidth)(percentage),
        [bodyWidth]
    );
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const columns: ColumnDef<TableProps>[] = useMemo(
        () => [
            {
                header: "@id",
                accessorKey: "documentId",
                cell: ({ getValue }) => (
                    <CellDocumentValue value={getValue()} databaseName={databaseName} hasHyperlinkForIds />
                ),
                size: getSize(25),
            },
            {
                header: "Error message",
                accessorKey: "error",
                cell: CellWithCopyWrapper,
                size: getSize(75),
            },
        ],
        [getSize]
    );

    return { columns };
}

const schema = yup.object({
    isTestSettingsEnabled: yup.boolean(),
    maxDocumentsToValidate: yup.number().nullable().positive(),
    maxErrorMessages: yup.number().nullable().positive(),
});

export type ValidateSchemaFormData = yup.InferType<typeof schema>;

interface ValidationTotals {
    totalValidated: number;
    totalTarget: number;
}

function getCollectionDocumentCount(
    collectionName: string,
    collections: { name: string; documentCount: number }[]
): number {
    return collections.find((c) => c.name === collectionName)?.documentCount ?? 0;
}

function calculateTargetDocumentCount(
    collectionName: string,
    collections: { name: string; documentCount: number }[],
    isTestSettingsEnabled: boolean,
    maxDocumentsToValidate: number | null
): number {
    const docCount = getCollectionDocumentCount(collectionName, collections);
    const maxDocs = isTestSettingsEnabled ? maxDocumentsToValidate : null;
    return typeof maxDocs === "number" && maxDocs > 0 ? Math.min(docCount, maxDocs) : docCount;
}

function calculateValidationTotals(
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[],
    monitorOperationProgress: Record<string, ValidationOperationProgress>,
    collections: { name: string; documentCount: number }[],
    isTestSettingsEnabled: boolean,
    maxDocumentsToValidate: number | null
): ValidationTotals {
    const totalValidated = validators.reduce((sum, v) => {
        return sum + (monitorOperationProgress?.[v.Name]?.ValidatedCount ?? 0);
    }, 0);

    const totalTarget = validators.reduce((sum, v) => {
        const targetForCol = calculateTargetDocumentCount(
            v.Name,
            collections,
            isTestSettingsEnabled,
            maxDocumentsToValidate
        );
        return sum + targetForCol;
    }, 0);

    return { totalValidated, totalTarget };
}

interface ValidationProgressSummaryProps {
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[];
    monitorOperationProgress: Record<string, ValidationOperationProgress>;
    collections: { name: string; documentCount: number }[];
    isTestSettingsEnabled: boolean;
    maxDocumentsToValidate: number | null;
}

function ValidationProgressSummary({
    validators,
    monitorOperationProgress,
    collections,
    isTestSettingsEnabled,
    maxDocumentsToValidate,
}: ValidationProgressSummaryProps) {
    const { totalValidated, totalTarget } = calculateValidationTotals(
        validators,
        monitorOperationProgress,
        collections,
        isTestSettingsEnabled,
        maxDocumentsToValidate
    );

    return (
        <div>
            <div className="hstack justify-content-between mb-1">
                <h4 className="mb-0">Running validation schema test...</h4>
                <small className="text-muted text-nowrap">
                    {totalValidated} / {totalTarget} documents tested
                </small>
            </div>
            <ProgressBar max={totalTarget} min={0} now={totalValidated} />
        </div>
    );
}

interface ValidationStatusMessageProps {
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[];
    monitorOperationProgress: Record<string, ValidationOperationProgress>;
}

function ValidationStatusMessage({ validators, monitorOperationProgress }: ValidationStatusMessageProps) {
    if (validators.some((v) => monitorOperationProgress[v.Name]?.status === "error")) {
        return (
            <div>
                <Icon icon="danger" color="danger" />
                An unexpected error occurred during validation.
            </div>
        );
    }

    if (isEmpty(monitorOperationProgress)) {
        return (
            <div>You&#39;re about to test your documents against the sample schema for the following collections:</div>
        );
    }

    const totalErrors = validators.reduce((sum, v) => {
        return sum + (monitorOperationProgress?.[v.Name]?.ErrorCount ?? 0);
    }, 0);

    if (totalErrors > 0) {
        return (
            <div>
                <Icon icon="warning" className="text-warning" />
                <b className="text-warning">
                    {totalErrors} invalid document
                    {totalErrors !== 1 ? "s " : " "}
                </b>
                <span>has been found based on the sample schemas.</span>
            </div>
        );
    }

    return (
        <div>
            <span className="text-success">
                <Icon icon="check" />
                <b>All documents validated successfully </b>
            </span>
            <span>against the sample schemas. No errors found.</span>
        </div>
    );
}

interface ValidationDocumentCountDisplayProps {
    collectionName: string;
    errorCount: number;
    collections: { name: string; documentCount: number }[];
    isTestSettingsEnabled: boolean;
    maxDocumentsToValidate: number | null;
}

function ValidationDocumentCountDisplay({
    collectionName,
    errorCount,
    collections,
    isTestSettingsEnabled,
    maxDocumentsToValidate,
}: ValidationDocumentCountDisplayProps) {
    if (errorCount > 0) {
        const targetCount = calculateTargetDocumentCount(
            collectionName,
            collections,
            isTestSettingsEnabled,
            maxDocumentsToValidate
        );
        return <span> (out of {targetCount} documents)</span>;
    }

    const docCount = getCollectionDocumentCount(collectionName, collections);
    return (
        <span>
            {" "}
            ({docCount}) document{docCount === 1 ? "" : "s"}
        </span>
    );
}

interface ValidationCollectionAccordionItemProps {
    validator: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">;
    monitorOperationProgress: ValidationOperationProgress;
    collections: { name: string; documentCount: number }[];
    isTestSettingsEnabled: boolean;
    maxDocumentsToValidate: number | null;
    selected: boolean;
    toggleSelection: (x: ChangeEvent<HTMLInputElement>) => void;
    showSelection?: boolean;
}

function ValidationCollectionAccordionItem({
    validator,
    monitorOperationProgress,
    collections,
    isTestSettingsEnabled,
    maxDocumentsToValidate,
    selected,
    toggleSelection,
    showSelection,
}: ValidationCollectionAccordionItemProps) {
    const errorCount = monitorOperationProgress?.ErrorCount ?? 0;
    const isCompleted = monitorOperationProgress?.status === "complete";
    const isLoading = monitorOperationProgress?.status === "loading";
    const isErrored = monitorOperationProgress?.status === "error";

    const errorMessage = isErrored
        ? ((monitorOperationProgress.error as any)?.Message ?? String(monitorOperationProgress.error))
        : null;
    const fullError = isErrored
        ? ((monitorOperationProgress.error as any)?.Error ?? JSON.stringify(monitorOperationProgress.error, null, 2))
        : null;

    const handleCheckboxChange = (e: ChangeEvent<HTMLInputElement>) => {
        toggleSelection(e);
    };

    return (
        <Accordion.Item eventKey={validator.Name}>
            <Accordion.Header
                as="h5"
                className={classNames("border border-secondary", {
                    "hide-accordion-dropdown": !isCompleted || (isCompleted && errorCount === 0 && !isErrored),
                })}
            >
                <div className="hstack gap-2">
                    {showSelection && (
                        <Checkbox
                            color="primary"
                            className="mb-0"
                            selected={selected}
                            toggleSelection={handleCheckboxChange}
                            disabled={isLoading}
                            onClick={(e) => e.stopPropagation()} // Prevent checkbox click from propagating to accordion header
                        />
                    )}
                    <span>{validator.Name}</span>{" "}
                </div>
                <small className="ms-1 text-muted text-truncate flex-grow-1">
                    {isErrored && (
                        <b className="text-danger">
                            <Icon icon="danger" color="danger" />
                            {errorMessage}
                        </b>
                    )}
                    {isCompleted && errorCount === 0 && (
                        <b className="text-success">
                            <Icon icon="check" />
                            <span>All valid</span>
                        </b>
                    )}

                    {isCompleted && errorCount > 0 && (
                        <b className="text-warning">
                            <Icon icon="warning" />
                            <span>{errorCount} invalid</span>
                        </b>
                    )}

                    {!isErrored && (
                        <span>
                            <ValidationDocumentCountDisplay
                                collectionName={validator.Name}
                                errorCount={errorCount}
                                collections={collections}
                                isTestSettingsEnabled={isTestSettingsEnabled}
                                maxDocumentsToValidate={maxDocumentsToValidate}
                            />
                        </span>
                    )}
                </small>
                {isLoading && <Spinner size="sm" className="spinner-gradient" />}
            </Accordion.Header>
            {isErrored && (
                <Accordion.Collapse
                    className="border border-secondary"
                    unmountOnExit
                    mountOnEnter
                    eventKey={validator.Name}
                >
                    <Accordion.Body>
                        <Code code={fullError} language="plaintext" />
                    </Accordion.Body>
                </Accordion.Collapse>
            )}
            {!isErrored && isCompleted && errorCount > 0 && (
                <Accordion.Collapse
                    className="border border-secondary"
                    unmountOnExit
                    mountOnEnter
                    eventKey={validator.Name}
                >
                    <Accordion.Body>
                        <SizeGetter
                            render={(props) => (
                                <ValidatedDocumentsTable
                                    loading={isLoading}
                                    result={monitorOperationProgress}
                                    {...props}
                                />
                            )}
                        />
                    </Accordion.Body>
                </Accordion.Collapse>
            )}
        </Accordion.Item>
    );
}

function getIsCollectionSelected(name: string, selectedCollections: Record<string, boolean>) {
    return selectedCollections[name];
}

function getSelectedValidators(
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[],
    selectedCollections: Record<string, boolean>
): Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[] {
    return validators.filter((v) => getIsCollectionSelected(v.Name, selectedCollections));
}
