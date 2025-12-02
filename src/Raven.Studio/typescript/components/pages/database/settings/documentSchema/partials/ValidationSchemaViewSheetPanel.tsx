import { ViewSheet } from "components/common/splitView/ViewSheet";
import { Icon } from "components/common/Icon";
import React, { useMemo } from "react";
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

interface ValidationSchemaViewSheetPanelProps {
    validators: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">[];
}

interface ValidationOperationProgress extends ValidateSchemaResult {
    completed?: boolean;
}

export function ValidationSchemaViewSheetPanel({ validators }: ValidationSchemaViewSheetPanelProps) {
    // TODO: At the moment, when i close viewSheet, the entire state is deleted. I need to add the ability to persist this state if, for example, someone runs a test.
    const [monitorOperationProgress, setMonitorOperationProgress] = React.useState<
        Record<string, ValidationOperationProgress>
    >({});
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const collections = useAppSelector(collectionsTrackerSelectors.collections);

    const form = useForm<ValidateSchemaFormData>({
        resolver: yupResolver(schema),
    });

    const asyncGetOperationIds = useAsyncCallback(async (formData: ValidateSchemaFormData) => {
        const dtos: ValidateSchemaRequestDto[] = validators.map((validator) =>
            documentSchemaUtils.mapToValidateSchemaRequestDto(validator, formData)
        );

        return await Promise.all(dtos.map(async (dto) => await databasesService.validateSchema(databaseName, dto)));
    });

    const asyncTestSchema = useAsyncCallback(async (formData: ValidateSchemaFormData) => {
        setMonitorOperationProgress({});

        const dtos: ValidateSchemaRequestDto[] = validators.map((validator) =>
            documentSchemaUtils.mapToValidateSchemaRequestDto(validator, formData)
        );

        const operationResults = await asyncGetOperationIds.execute(formData);

        const monitorPromises = operationResults.map((result, idx) => {
            const collectionName = dtos[idx].Collection;

            return notificationCenter.instance.monitorOperation<ValidationOperationProgress>(
                databaseName,
                result.OperationId,
                (progress) =>
                    setMonitorOperationProgress((prev) => ({
                        ...prev,
                        [collectionName]: progress,
                    }))
            );
        });

        const finalResults = await Promise.all(monitorPromises);

        setMonitorOperationProgress((prev) => {
            const next = { ...prev };

            finalResults.forEach((result, idx) => {
                const collectionName = dtos[idx].Collection;
                next[collectionName] = { ...next[collectionName], ...result, completed: true };
            });

            return next;
        });

        return finalResults;
    });

    const killOperation = useAsyncCallback(async () => {
        if (!asyncGetOperationIds.result) {
            return [];
        }

        return Promise.all(
            asyncGetOperationIds.result.map(async (result) =>
                databasesService.killOperation(databaseName, result.OperationId)
            )
        );
    });
    const { control, handleSubmit } = form;

    const formValues = useWatch({
        control,
    });

    const isTestSettingsDisabled = !formValues.isTestSettingsEnabled || asyncTestSchema.loading;

    return (
        <FormProvider {...form}>
            <form className="h-100" onSubmit={handleSubmit(asyncTestSchema.execute)}>
                <ViewSheet className="h-100 validation-schema-view-sheet-panel">
                    <ViewSheet.Header>
                        <div className="d-flex gap-2 align-items-center">
                            <Icon icon="rocket" size="lg" className="text-primary" />
                            <h3 className="mb-0">Validation schema test</h3>
                        </div>
                    </ViewSheet.Header>
                    <ViewSheet.Body className="p-4">
                        <h4 className="w-100 text-center">
                            {asyncTestSchema.loading ? (
                                <ValidationProgressSummary
                                    validators={validators}
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
                        <Accordion alwaysOpen className="mt-1" defaultActiveKey={[]}>
                            {validators.map((validator) => (
                                <ValidationCollectionAccordionItem
                                    key={validator.Name}
                                    validator={validator}
                                    monitorOperationProgress={monitorOperationProgress}
                                    collections={collections}
                                    isTestSettingsEnabled={formValues.isTestSettingsEnabled}
                                    maxDocumentsToValidate={formValues.maxDocumentsToValidate}
                                    isLoading={asyncTestSchema.loading}
                                />
                            ))}
                        </Accordion>
                        <div
                            className={classNames("mt-4", {
                                "item-disabled": asyncTestSchema.loading,
                            })}
                        >
                            <FormSwitch color="primary" control={control} name="isTestSettingsEnabled">
                                Test settings
                            </FormSwitch>
                            <div>Specify maximum documents and run time - leave unset for unlimited.</div>
                            <div
                                className={classNames("mt-2", {
                                    "item-disabled": isTestSettingsDisabled,
                                })}
                            >
                                <FormGroup>
                                    <FormLabel>Max documents to scan (per collection)</FormLabel>
                                    <FormInput
                                        name="maxDocumentsToValidate"
                                        control={control}
                                        disabled={isTestSettingsDisabled}
                                        addon="documents"
                                        placeholder="e.g. 1000"
                                        type="text"
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>Max error messages to return</FormLabel>
                                    <FormInput
                                        name="maxErrorMessages"
                                        control={control}
                                        disabled={isTestSettingsDisabled}
                                        placeholder="e.g. 1000"
                                        addon="documents"
                                        type="text"
                                    />
                                </FormGroup>
                            </div>
                        </div>
                    </ViewSheet.Body>
                    <ViewSheet.Footer className="d-flex justify-content-end">
                        {asyncTestSchema.loading ? (
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
                                isSpinning={asyncTestSchema.loading}
                                icon="start"
                                variant="primary"
                                type="submit"
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
    return <VirtualTable className="panel-bg-1" isLoading={loading} table={table} heightInPx={400} />;
};

function useValidationInvalidDocumentsColumns(availableWidth: number): { columns: ColumnDef<TableProps>[] } {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const columns: ColumnDef<TableProps>[] = useMemo(() => {
        const cols: ColumnDef<TableProps>[] = [
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
        ];

        return cols;
    }, []);

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
    console.log("maxym monitorOperationProgress", monitorOperationProgress);

    if (isEmpty(monitorOperationProgress)) {
        return <div>You&#39;re about to run the validation schema test on these collections:</div>;
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
                <span>been found according to the defined document schemas.</span>
            </div>
        );
    }

    return (
        <div>
            <span className="text-success">
                <Icon icon="check" />
                <b>All documents validated successfully </b>
            </span>
            <span>against the defined schemas with no errors found.</span>
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
    return <span> ({docCount}) documents</span>;
}

interface ValidationCollectionAccordionItemProps {
    validator: Pick<DocumentSchemaValidatorConfig, "Name" | "Schema">;
    monitorOperationProgress: Record<string, ValidationOperationProgress>;
    collections: { name: string; documentCount: number }[];
    isTestSettingsEnabled: boolean;
    maxDocumentsToValidate: number | null;
    isLoading: boolean;
}

function ValidationCollectionAccordionItem({
    validator,
    monitorOperationProgress,
    collections,
    isTestSettingsEnabled,
    maxDocumentsToValidate,
    isLoading,
}: ValidationCollectionAccordionItemProps) {
    const validationResult = monitorOperationProgress?.[validator.Name];
    const errorCount = validationResult?.ErrorCount ?? 0;
    const isCompleted = validationResult?.completed;

    return (
        <Accordion.Item className="border-light-var" eventKey={validator.Name}>
            <Accordion.Header as="h5" className={classNames({ "hide-accordion-dropdown": isLoading })}>
                <span>{validator.Name}</span>{" "}
                <small className="ms-3 text-muted flex-grow-1">
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

                    <span>
                        <ValidationDocumentCountDisplay
                            collectionName={validator.Name}
                            errorCount={errorCount}
                            collections={collections}
                            isTestSettingsEnabled={isTestSettingsEnabled}
                            maxDocumentsToValidate={maxDocumentsToValidate}
                        />
                    </span>
                </small>
                {isLoading && !isCompleted && <Spinner size="sm" className="spinner-gradient" />}
            </Accordion.Header>
            {!isLoading && (
                <Accordion.Collapse unmountOnExit mountOnEnter eventKey={validator.Name}>
                    <Accordion.Body>
                        <SizeGetter
                            render={(props) => (
                                <ValidatedDocumentsTable loading={isLoading} result={validationResult} {...props} />
                            )}
                        />
                    </Accordion.Body>
                </Accordion.Collapse>
            )}
        </Accordion.Item>
    );
}
