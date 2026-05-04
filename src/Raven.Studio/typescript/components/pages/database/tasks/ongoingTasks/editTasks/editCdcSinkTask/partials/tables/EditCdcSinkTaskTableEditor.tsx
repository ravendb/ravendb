import { EmptySet } from "components/common/EmptySet";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { FieldArrayPath, FieldName, FieldPath, useFieldArray, useFormContext } from "react-hook-form";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { FormGroup, FormInput, FormLabel, FormSelect } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type FormTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];

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
        </div>
    );
}

const columnTypeOptions: SelectOption<CdcColumnType>[] = [
    { value: "Default", label: "Default" },
    { value: "Json", label: "JSON" },
    { value: "Attachment", label: "Attachment" },
];

function LinkedTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const getLinkedFieldName = (field: keyof NonNullable<FormTable["linkedTables"]>[number]) =>
        getFormFieldPath(`${activeTable.current.path}.${field}`);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-6">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("propertyName")} />
                </FormGroup>
                <FormGroup className="g-col-6">
                    <FormLabel>Linked collection</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("linkedCollectionName")} />
                </FormGroup>
                <FormGroup className="g-col-6">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("sourceTableSchema")} />
                </FormGroup>
                <FormGroup className="g-col-6">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={getLinkedFieldName("sourceTableName")} />
                </FormGroup>
            </div>
        </div>
    );
}

function EmbeddedTableEditor() {
    const activeTable = useAppSelector(editCdcSinkTaskSelectors.activeTable);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const getEmbeddedFieldName = (field: keyof NonNullable<FormTable["embeddedTables"]>[number]) =>
        getFormFieldPath(`${activeTable.current.path}.${field}`);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-6">
                    <FormLabel>Property name</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("propertyName")} />
                </FormGroup>
                <FormGroup className="g-col-6">
                    <FormLabel>Source schema</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("sourceTableSchema")} />
                </FormGroup>
                <FormGroup className="g-col-6">
                    <FormLabel>Source table</FormLabel>
                    <FormInput type="text" control={control} name={getEmbeddedFieldName("sourceTableName")} />
                </FormGroup>
            </div>
        </div>
    );
}

function getFormFieldPath(path: string): FieldPath<EditCdcSinkTaskFormData> {
    return path as FieldPath<EditCdcSinkTaskFormData>;
}

function getFormFieldArrayPath(path: string): FieldArrayPath<EditCdcSinkTaskFormData> {
    return path as FieldArrayPath<EditCdcSinkTaskFormData>;
}
