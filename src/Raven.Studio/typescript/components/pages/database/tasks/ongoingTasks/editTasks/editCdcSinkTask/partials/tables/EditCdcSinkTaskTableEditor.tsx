import { EmptySet } from "components/common/EmptySet";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { FieldArrayPath, FieldName, FieldPath, useFieldArray, useFormContext, useWatch } from "react-hook-form";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FormAceEditor, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { ReactNode } from "react";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;
type FormTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];
type FormEmbeddedTable = NonNullable<FormTable["embeddedTables"]>[number];

export default function EditCdcSinkTaskTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    if (!activeTable) {
        return <EmptySet>Select a table to view its configuration details.</EmptySet>;
    }

    const key = activeTable.current.path;

    return (
        <div className="cdc-table-editor" key={key}>
            <div className="hstack p-2 border-bottom border-secondary">
                <Breadcrumb className="mb-0">
                    {activeTable.parents.map((parent, idx) => (
                        <Breadcrumb.Item key={idx}>{parent.label}</Breadcrumb.Item>
                    ))}
                    <Breadcrumb.Item active>{activeTable.current.label}</Breadcrumb.Item>
                </Breadcrumb>
                <Button variant="info" className="ms-auto">
                    <Icon icon="rocket" />
                    Test
                </Button>
            </div>
            <div className="p-2">
                {activeTable.current.type === "root" && <RootTableEditor />}
                {activeTable.current.type === "linked" && <LinkedTableEditor />}
                {activeTable.current.type === "embedded" && <EmbeddedTableEditor />}
            </div>
        </div>
    );
}

function RootTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const tablePath = activeTable.current.path;

    const getRootFieldName = (field: keyof FormTable) => getFormFieldPath(`${tablePath}.${field}`);

    const getColumnsFieldName = (idx: number, field: FieldName<NonNullable<FormTable["columns"]>[number]>) =>
        getFormFieldPath(`${tablePath}.columns.${idx}.${field}`);

    const columnsFieldArray = useFieldArray({
        control,
        name: getFormFieldArrayPath(`${tablePath}.columns`),
    });
    const hasAdvancedValues = useHasRootAdvancedValues(tablePath);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-4">
                    <FormLabel>Collection name</FormLabel>
                    <FormInput type="text" control={control} name={getRootFieldName("collectionName")} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={getRootFieldName("sourceTableSchema")} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={getRootFieldName("sourceTableName")} />
                </FormGroup>
            </div>
            <StringValueListEditor
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                fieldArrayPath={`${tablePath}.primaryKeyColumns`}
            />
            <div className="hstack justify-content-between mb-1">
                <div>Field mapping</div>
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => columnsFieldArray.append({ column: "", name: "", type: "Default" })}
                >
                    <Icon icon="plus" />
                    Add field mapping
                </Button>
            </div>
            <div>
                <div className="field-mapping-row panel-bg-2 rounded-top p-1">
                    <div>Source column</div>
                    <div></div>
                    <div>Target column</div>
                    <div>Type</div>
                    <div></div>
                </div>
                <div className="bg-body rounded-bottom p-1 vstack gap-1">
                    {columnsFieldArray.fields.map((field, idx) => (
                        <div key={field.id} className="field-mapping-row">
                            <FormInput type="text" control={control} name={getColumnsFieldName(idx, "column")} />
                            <Icon icon="arrow-thin-right" margin="m-0" />
                            <FormInput type="text" control={control} name={getColumnsFieldName(idx, "name")} />
                            <FormSelect
                                control={control}
                                name={getColumnsFieldName(idx, "type")}
                                options={columnTypeOptions}
                            />
                            <Button
                                variant="link"
                                className="text-danger"
                                size="sm"
                                onClick={() => columnsFieldArray.remove(idx)}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        </div>
                    ))}
                </div>
            </div>
            <AdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <PatchAdvancedField tablePath={tablePath} />
                <OnDeleteAdvancedFields tablePath={tablePath} />
            </AdvancedSettings>
        </div>
    );
}

const columnTypeOptions: SelectOption<CdcColumnType>[] = [
    { value: "Default", label: "Default" },
    { value: "Json", label: "JSON" },
    { value: "Attachment", label: "Attachment" },
];

const relationTypeOptions: SelectOption<CdcSinkRelationType>[] = [
    { value: "Array", label: "Array" },
    { value: "Map", label: "Map" },
    { value: "Value", label: "Value" },
];

function LinkedTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const getLinkedFieldName = (field: keyof NonNullable<FormTable["linkedTables"]>[number]) =>
        getFormFieldPath(`${activeTable.current.path}.${field}`);

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("propertyName")} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Linked collection</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("linkedCollectionName")} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("sourceTableSchema")} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("sourceTableName")} />
                </FormGroup>
            </div>
            <StringValueListEditor
                title="Join columns"
                addButtonLabel="Add join column"
                fieldArrayPath={`${activeTable.current.path}.joinColumns`}
            />
        </div>
    );
}

function EmbeddedTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const tablePath = activeTable.current.path;

    const getEmbeddedFieldName = (field: keyof FormEmbeddedTable) => getFormFieldPath(`${tablePath}.${field}`);
    const hasAdvancedValues = useHasEmbeddedAdvancedValues(tablePath);

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("propertyName")} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Relation type</FormLabel>
                    <FormSelect control={control} name={getEmbeddedFieldName("type")} options={relationTypeOptions} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("sourceTableSchema")} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("sourceTableName")} />
                </FormGroup>
            </div>
            <StringValueListEditor
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                fieldArrayPath={`${tablePath}.primaryKeyColumns`}
            />
            <StringValueListEditor
                title="Join columns"
                addButtonLabel="Add join column"
                fieldArrayPath={`${tablePath}.joinColumns`}
            />
            <AdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <FormGroup>
                    <FormSwitch control={control} name={getEmbeddedFieldName("caseSensitiveKeys")}>
                        Case sensitive keys
                    </FormSwitch>
                </FormGroup>
                <PatchAdvancedField tablePath={tablePath} />
                <OnDeleteAdvancedFields tablePath={tablePath} />
            </AdvancedSettings>
        </div>
    );
}

interface StringValueListEditorProps {
    title: string;
    addButtonLabel: string;
    fieldArrayPath: string;
}

function StringValueListEditor({ title, addButtonLabel, fieldArrayPath }: StringValueListEditorProps) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const fieldArray = useFieldArray({
        control,
        name: getFormFieldArrayPath(fieldArrayPath),
    });

    const getValueFieldName = (idx: number) => getFormFieldPath(`${fieldArrayPath}.${idx}.value`);

    return (
        <div className="mb-2">
            <div className="hstack justify-content-between mb-1">
                <div>{title}</div>
                <Button variant="link" size="sm" onClick={() => fieldArray.append({ value: "" })}>
                    <Icon icon="plus" />
                    {addButtonLabel}
                </Button>
            </div>
            <div className="vstack gap-1">
                {fieldArray.fields.map((field, idx) => (
                    <div key={field.id} className="hstack gap-1">
                        <FormInput type="text" control={control} name={getValueFieldName(idx)} />
                        <Button variant="link" className="text-danger" size="sm" onClick={() => fieldArray.remove(idx)}>
                            <Icon icon="trash" margin="m-0" />
                        </Button>
                    </div>
                ))}
            </div>
        </div>
    );
}

interface AdvancedSettingsProps {
    hasAdvancedValues: boolean;
    children: ReactNode;
}

function AdvancedSettings({ hasAdvancedValues, children }: AdvancedSettingsProps) {
    return (
        <Accordion defaultActiveKey={hasAdvancedValues ? "advanced-settings" : null} className="mt-2">
            <Accordion.Item eventKey="advanced-settings" className="border border-secondary rounded-2 panel-bg-2">
                <Accordion.Header
                    as={() => (
                        <AccordionButton className="rounded-2 panel-bg-2 fs-5 p-1">Advanced settings</AccordionButton>
                    )}
                ></Accordion.Header>
                <Accordion.Body className="p-2">
                    <div className="vstack gap-2">{children}</div>
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
}

function OnDeleteAdvancedFields({ tablePath }: { tablePath: string }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const isIgnoreDeletes = useWatch({ control, name: getFormFieldPath(`${tablePath}.onDelete.ignoreDeletes`) });

    return (
        <div>
            <FormGroup>
                <FormSwitch control={control} name={getFormFieldPath(`${tablePath}.onDelete.ignoreDeletes`)}>
                    Ignore deletes
                </FormSwitch>
            </FormGroup>
            <FormGroup marginClass="mb-0">
                <FormLabel>Delete patch</FormLabel>
                <FormAceEditor
                    control={control}
                    name={getFormFieldPath(`${tablePath}.onDelete.patch`)}
                    mode="javascript"
                    disabled={Boolean(isIgnoreDeletes)}
                />
            </FormGroup>
        </div>
    );
}

function PatchAdvancedField({ tablePath }: { tablePath: string }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <FormGroup marginClass="mb-0">
            <FormLabel>Patch</FormLabel>
            <FormAceEditor control={control} name={getFormFieldPath(`${tablePath}.patch`)} mode="javascript" />
        </FormGroup>
    );
}

function useHasRootAdvancedValues(tablePath: string) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const disabled = useWatch({ control, name: getFormFieldPath(`${tablePath}.disabled`) });
    const patch = useWatch({ control, name: getFormFieldPath(`${tablePath}.patch`) });
    const ignoreDeletes = useWatch({ control, name: getFormFieldPath(`${tablePath}.onDelete.ignoreDeletes`) });
    const deletePatch = useWatch({ control, name: getFormFieldPath(`${tablePath}.onDelete.patch`) });

    return Boolean(disabled || patch || ignoreDeletes || deletePatch);
}

function useHasEmbeddedAdvancedValues(tablePath: string) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const relationType = useWatch({ control, name: getFormFieldPath(`${tablePath}.type`) });
    const caseSensitiveKeys = useWatch({ control, name: getFormFieldPath(`${tablePath}.caseSensitiveKeys`) });
    const patch = useWatch({ control, name: getFormFieldPath(`${tablePath}.patch`) });
    const ignoreDeletes = useWatch({ control, name: getFormFieldPath(`${tablePath}.onDelete.ignoreDeletes`) });
    const deletePatch = useWatch({ control, name: getFormFieldPath(`${tablePath}.onDelete.patch`) });

    return Boolean(
        (relationType && relationType !== "Array") || caseSensitiveKeys || patch || ignoreDeletes || deletePatch
    );
}

function getFormFieldPath(path: string): FieldPath<EditCdcSinkTaskFormData> {
    return path as FieldPath<EditCdcSinkTaskFormData>;
}

function getFormFieldArrayPath(path: string): FieldArrayPath<EditCdcSinkTaskFormData> {
    return path as FieldArrayPath<EditCdcSinkTaskFormData>;
}
