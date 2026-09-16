import { EmptySet } from "components/common/EmptySet";
import ExpandableList from "components/common/ExpandableList";
import { FormErrorIcon, FormGroup, FormInput, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import useBoolean from "components/hooks/useBoolean";
import {
    RootTablePath,
    EmbeddedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { useFieldArray, useFormContext } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type FieldMappingPath = RootTablePath | EmbeddedTablePath;

export default function EditCdcSinkTaskFieldMapping({ path }: { path: FieldMappingPath }) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const isFieldMappingExpandedByDefault = useAppSelector(editCdcSinkTaskSelectors.isFieldMappingExpandedByDefault);
    const { value: isExpanded, setTrue: expand, setValue: setIsExpanded } = useBoolean(isFieldMappingExpandedByDefault);

    const columnsPath = `${path}.columns` as const;

    const columnsFieldArray = useFieldArray({
        control,
        name: columnsPath,
    });

    return (
        <>
            <div className="hstack justify-content-between mb-1">
                <div className="hstack">
                    Field mapping
                    <FormErrorIcon control={control} paths={[columnsPath]} />
                </div>
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => {
                        columnsFieldArray.append({ column: "", name: "", type: "Default" });
                        expand();
                    }}
                >
                    <Icon icon="plus" />
                    Add field mapping
                </Button>
            </div>
            {columnsFieldArray.fields.length === 0 ? (
                <div className="panel-bg-2 p-1 rounded border border-secondary hstack justify-content-center">
                    <EmptySet compact>No field mappings defined.</EmptySet>
                </div>
            ) : (
                <div>
                    <div className="field-mapping-row panel-bg-2 rounded-top p-1">
                        <div>Source column</div>
                        <div></div>
                        <div>
                            Target property
                            <PopoverWithHoverWrapper message="The RavenDB document field where the source column value will be stored.">
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <div>
                            Type
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        How the source column value is stored in the document:
                                        <br />
                                        <strong>Default</strong> - stored as a regular property.
                                        <br />
                                        <strong>JSON</strong> - parsed as JSON before storing.
                                        <br />
                                        <strong>Attachment</strong> - stored as a RavenDB attachment.
                                    </>
                                }
                            >
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <div></div>
                    </div>
                    <ExpandableList
                        className="bg-body rounded-bottom p-1"
                        contentClassName="vstack gap-1"
                        itemsCount={columnsFieldArray.fields.length}
                        collapsedItemsCount={6}
                        isExpanded={isExpanded}
                        setIsExpanded={setIsExpanded}
                    >
                        {({ visibleCount }) =>
                            columnsFieldArray.fields.slice(0, visibleCount).map((field, idx) => (
                                <div key={field.id} className="field-mapping-row">
                                    <FormGroup marginClass="m-0">
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name={`${path}.columns.${idx}.column`}
                                        />
                                    </FormGroup>
                                    <Icon icon="arrow-thin-right" margin="m-0" />
                                    <FormGroup marginClass="m-0">
                                        <FormInput type="text" control={control} name={`${path}.columns.${idx}.name`} />
                                    </FormGroup>
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
                            ))
                        }
                    </ExpandableList>
                </div>
            )}
        </>
    );
}

const columnTypeOptions: SelectOption<CdcColumnType>[] = [
    { value: "Default", label: "Default" },
    { value: "Json", label: "JSON" },
    { value: "Attachment", label: "Attachment" },
];
