import { FormGroup, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import RichAlert from "components/common/RichAlert";
import { useEditCdcSinkTaskSourceTableAutoFill } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskSourceTableAutoFill";
import EditCdcSinkTaskWarningMessage from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskWarningMessage";
import {
    analyzeRootTables,
    getLinkedTableWarningMessagesFromAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import { LinkedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";

export default function EditCdcSinkTaskLinkedTableEditor({ path }: { path: LinkedTablePath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const linkedTable = useWatch({ control, name: path });
    const rootTables = useWatch({ control, name: "tables" });
    const { handleSourceTableChange, sourceSchemaOptions, sourceTableOptions } = useEditCdcSinkTaskSourceTableAutoFill(
        path,
        "linked"
    );

    const rootTablesAnalysis = useMemo(() => analyzeRootTables(rootTables), [rootTables]);
    const warningMessages = useMemo(
        () => getLinkedTableWarningMessagesFromAnalysis(rootTablesAnalysis, linkedTable),
        [rootTablesAnalysis, linkedTable]
    );

    return (
        <div>
            {warningMessages.map((warning) => (
                <RichAlert key={warning} variant="warning" className="mb-3">
                    <EditCdcSinkTaskWarningMessage message={warning} />
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
                        <PopoverWithHoverWrapper message="The RavenDB document field that will hold the reference to the linked (related) document.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        Linked collection
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    The RavenDB collection name for the related documents.
                                    <br />
                                    The related document ID is derived from this collection name and the join column
                                    values.
                                </>
                            }
                        >
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.linkedCollectionName`} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        Join columns
                        <PopoverWithHoverWrapper message="The foreign key columns used to join this linked table to the parent table. Their values, combined with the linked collection name, form the related document ID.">
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel="Add join column"
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
            />
        </div>
    );
}
