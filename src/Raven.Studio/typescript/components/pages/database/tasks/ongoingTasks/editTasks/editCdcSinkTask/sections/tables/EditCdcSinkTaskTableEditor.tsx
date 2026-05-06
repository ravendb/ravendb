import { EmptySet } from "components/common/EmptySet";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { FieldPath, useFieldArray, useFormContext, useWatch } from "react-hook-form";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FormAceEditor, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { ReactNode } from "react";
import {
    RootTablePath,
    LinkedTablePath,
    EmbeddedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;

export default function EditCdcSinkTaskTableEditor() {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);

    if (!activeTable) {
        return <EmptySet>Select a table to view its configuration details.</EmptySet>;
    }

    const currentPath = activeTable.path;

    return (
        <div className="cdc-table-editor" key={currentPath}>
            <div className="hstack p-2 border-bottom border-secondary">
                {/* <Breadcrumb className="mb-0">
                    {breadcrumbs.map((part, idx) => (
                        <Breadcrumb.Item key={idx} active={idx === breadcrumbs.length - 1}>
                            {part.fieldName}
                        </Breadcrumb.Item>
                    ))}
                </Breadcrumb> */}
                <div className="ms-auto hstack gap-2 align-items-center">
                    {activeTable.type === "root" && (
                        <FormSwitch control={control} name={`${activeTable.path}.disabled`}>
                            Disabled
                        </FormSwitch>
                    )}
                    <Button variant="info">
                        <Icon icon="rocket" />
                        Test
                    </Button>
                </div>
            </div>
            <div className="p-2">
                {activeTable.type === "root" && <RootTableEditor path={activeTable.path} />}
                {activeTable.type === "linked" && <LinkedTableEditor path={activeTable.path} />}
                {activeTable.type === "embedded" && <EmbeddedTableEditor path={activeTable.path} />}
            </div>
        </div>
    );
}

function RootTableEditor({ path }: { path: RootTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const columnsFieldArray = useFieldArray({
        control,
        name: `${path}.columns`,
    });

    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(patch || ignoreDeletes || deletePatch);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-4">
                    <FormLabel>Collection name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.collectionName`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
            </div>
            <StringValueListEditor
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                path={`${path}.primaryKeyColumns`}
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
                            <FormInput type="text" control={control} name={`${path}.columns.${idx}.column`} />
                            <Icon icon="arrow-thin-right" margin="m-0" />
                            <FormInput type="text" control={control} name={`${path}.columns.${idx}.name`} />
                            <FormSelect
                                control={control}
                                name={`${path}.columns.${idx}.type`}
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
                <PatchAdvancedField path={path} />
                <OnDeleteFields path={path} />
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

function LinkedTableEditor({ path }: { path: LinkedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Linked collection</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.linkedCollectionName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
            </div>
            <StringValueListEditor title="Join columns" addButtonLabel="Add join column" path={`${path}.joinColumns`} />
        </div>
    );
}

function EmbeddedTableEditor({ path }: { path: EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const caseSensitiveKeys = useWatch({ control, name: `${path}.caseSensitiveKeys` });
    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(caseSensitiveKeys || patch || ignoreDeletes || deletePatch);

    return (
        <div>
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Relation type</FormLabel>
                    <FormSelect control={control} name={`${path}.type`} options={relationTypeOptions} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
            </div>
            <StringValueListEditor
                title="Primary key columns"
                addButtonLabel="Add primary key column"
                path={`${path}.primaryKeyColumns`}
            />
            <StringValueListEditor title="Join columns" addButtonLabel="Add join column" path={`${path}.joinColumns`} />
            <AdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <FormGroup>
                    <FormSwitch control={control} name={`${path}.caseSensitiveKeys`}>
                        Case sensitive keys
                    </FormSwitch>
                </FormGroup>
                <PatchAdvancedField path={path} />
                <OnDeleteFields path={path} />
            </AdvancedSettings>
        </div>
    );
}

type PrimaryKeyColumnsPath<T = FieldPath<EditCdcSinkTaskFormData>> = T extends `${string}primaryKeyColumns` ? T : never;
type JoinColumnsPath<T = FieldPath<EditCdcSinkTaskFormData>> = T extends `${string}joinColumns` ? T : never;

interface StringValueListEditorProps {
    title: string;
    addButtonLabel: string;
    path: PrimaryKeyColumnsPath | JoinColumnsPath;
}

function StringValueListEditor({ title, addButtonLabel, path }: StringValueListEditorProps) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const fieldArray = useFieldArray({
        control,
        name: path,
    });

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
                        <FormInput type="text" control={control} name={`${path}.${idx}.value`} />
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

function OnDeleteFields({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const isIgnoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });

    return (
        <div>
            <FormGroup>
                <FormSwitch control={control} name={`${path}.onDelete.ignoreDeletes`}>
                    Ignore deletes
                </FormSwitch>
            </FormGroup>
            <FormGroup marginClass="mb-0">
                <FormLabel>Delete patch</FormLabel>
                <FormAceEditor
                    control={control}
                    name={`${path}.onDelete.patch`}
                    mode="javascript"
                    disabled={Boolean(isIgnoreDeletes)}
                />
            </FormGroup>
        </div>
    );
}

function PatchAdvancedField({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <FormGroup marginClass="mb-0">
            <FormLabel>Patch</FormLabel>
            <FormAceEditor control={control} name={`${path}.patch`} mode="javascript" />
        </FormGroup>
    );
}
