import {
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSwitch,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { useEditCdcSinkTaskSourceTableAutoFill } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskSourceTableAutoFill";
import {
    analyzeRootTables,
    getEmbeddedTableWarningMessagesFromAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import { EmbeddedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";

type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;

export default function EditCdcSinkTaskEmbeddedTableEditor({ path }: { path: EmbeddedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const caseSensitiveKeys = useWatch({ control, name: `${path}.caseSensitiveKeys` });
    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });
    const embeddedTable = useWatch({ control, name: path });
    const rootTables = useWatch({ control, name: "tables" });
    const { handleSourceTableChange, sourceSchemaOptions, sourceTableOptions } = useEditCdcSinkTaskSourceTableAutoFill(
        path,
        "embedded"
    );

    const hasAdvancedValues = Boolean(caseSensitiveKeys || patch || ignoreDeletes || deletePatch);
    const rootTablesAnalysis = useMemo(() => analyzeRootTables(rootTables), [rootTables]);
    const warningMessages = useMemo(
        () => getEmbeddedTableWarningMessagesFromAnalysis(rootTablesAnalysis, embeddedTable),
        [rootTablesAnalysis, embeddedTable]
    );

    return (
        <div>
            {warningMessages.map((warning) => (
                <RichAlert key={warning} variant="warning" className="mb-3">
                    {warning}
                </RichAlert>
            ))}
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source schema</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name={`${path}.sourceTableSchema`}
                        options={sourceSchemaOptions}
                        placeholder="Select or enter source schema"
                    />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>Source table</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name={`${path}.sourceTableName`}
                        options={sourceTableOptions}
                        afterSelect={handleSourceTableChange}
                        placeholder="Select or enter source table"
                    />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        Target property
                        <PopoverWithHoverWrapper message="The RavenDB document field where the embedded related data will be stored.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        Relation type
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    How related rows are embedded in the parent document:
                                    <br />
                                    <strong>Array</strong> - multiple rows stored as an array of objects.
                                    <br />
                                    <strong>Map</strong> - multiple rows stored as a keyed object (dictionary).
                                    <br />
                                    <strong>Value</strong> - a single row stored directly as an object.
                                </>
                            }
                        >
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormSelect control={control} name={`${path}.type`} options={relationTypeOptions} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        Primary key columns
                        <PopoverWithHoverWrapper message="Columns that uniquely identify rows in this related table.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel="Add primary key column"
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldNameAccessor={(idx) => `${path}.primaryKeyColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <FormStringValueList
                title={
                    <>
                        Join columns
                        <PopoverWithHoverWrapper message="Columns used to match rows in this related table with rows from the root table.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel="Add join column"
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <FormGroup>
                    <FormSwitch control={control} name={`${path}.caseSensitiveKeys`}>
                        Case sensitive keys
                    </FormSwitch>
                </FormGroup>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}

const relationTypeOptions: SelectOption<CdcSinkRelationType>[] = [
    { value: "Array", label: "Array" },
    { value: "Map", label: "Map" },
    { value: "Value", label: "Value" },
];
