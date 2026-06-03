import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Code from "components/common/Code";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import InnerForm from "components/common/InnerForm";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import { useServices } from "components/hooks/useServices";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { RootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useEffect } from "react";
import { useAsyncCallback, UseAsyncReturn } from "react-async-hook";
import Accordion from "react-bootstrap/Accordion";
import AccordionButton from "react-bootstrap/AccordionButton";
import { useForm, UseFormReturn, useWatch } from "react-hook-form";
import * as yup from "yup";
import ExpandableListContainer from "components/common/ExpandableListContainer";
import FormStringValueList from "components/common/formFields/FormStringValueList";

type TestCdcSinkRowSelector = Raven.Client.Documents.Operations.CdcSink.Test.TestCdcSinkRowSelector;

interface EditCdcSinkTaskTestPanelProps {
    editForm: UseFormReturn<EditCdcSinkTaskFormData>;
    path: RootTablePath;
}

export default function EditCdcSinkTaskTestPanel({ editForm, path }: EditCdcSinkTaskTestPanelProps) {
    const taskId = useAppSelector(editCdcSinkTaskSelectors.taskId);
    const selectedConnectionString = useAppSelector(editCdcSinkTaskSelectors.selectedConnectionString);
    const table = editForm.watch(path);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async (formData: TestFormData) => {
        const formValues = editForm.getValues();
        const config = editCdcSinkTaskUtils.mapToDto(formValues, taskId);

        config.Name ||= "Test-CDC";

        return await tasksService.testCdcSink(databaseName, {
            Connection: selectedConnectionString,
            Configuration: config,
            Operation: formData.isSimulateOnDelete ? "Delete" : "Upsert",
            MaxRows: formData.rowSelector === "First" ? formData.maxRows : 1,
            PrimaryKeyValues: formData.primaryKeyValues.map((v) => v.value),
            RowSelector: formData.rowSelector,
            SourceTableName: table.sourceTableName,
            SourceTableSchema: table.sourceTableSchema,
        });
    });

    const testFormDefaultValues: TestFormData = {
        maxRows: 1,
        rowSelector: "First",
        primaryKeyValues: [{ value: "" }],
        isSimulateOnDelete: false,
    };

    const testForm = useForm<TestFormData>({
        defaultValues: testFormDefaultValues,
        resolver: yupResolver(testSchema),
    });

    const handleSubmit = testForm.handleSubmit(asyncTest.execute);

    const rowSelector = useWatch({
        control: testForm.control,
        name: "rowSelector",
    });

    // Auto submit on path change
    useEffect(() => {
        handleSubmit();
    }, [path]);

    return (
        <ViewSheet className="h-100">
            <InnerForm onSubmit={handleSubmit} className="vstack h-100">
                <ViewSheet.Header>
                    <h5 className="mb-0">
                        <Icon icon="map" />
                        Test mapping
                    </h5>
                </ViewSheet.Header>
                <ViewSheet.Body className="vstack gap-2">
                    <div className="small font-monospace hstack align-items-center justify-content-center bg-faded-info rounded px-1 flex-wrap text-center word-break">
                        {table.sourceTableName}
                        <Icon icon="arrow-thin-right" margin="mx-1" />
                        {table.collectionName}
                    </div>
                    <Accordion>
                        <Accordion.Item
                            eventKey="test-settings"
                            className="border border-secondary rounded-2 panel-bg-2"
                        >
                            <Accordion.Header
                                as={() => (
                                    <AccordionButton className="rounded-2 panel-bg-2 fs-5 p-1">
                                        Settings
                                        <FormErrorIcon
                                            control={testForm.control}
                                            paths={["maxRows", "rowSelector", "primaryKeyValues"]}
                                        />
                                    </AccordionButton>
                                )}
                            ></Accordion.Header>
                            <Accordion.Body className="p-2">
                                <div className="vstack gap-2">
                                    <FormGroup marginClass="m-0">
                                        <FormLabel>Row selector</FormLabel>
                                        <FormSelect
                                            control={testForm.control}
                                            name="rowSelector"
                                            options={rowSelectorOptions}
                                        />
                                    </FormGroup>
                                    {rowSelector === "First" && (
                                        <FormGroup marginClass="m-0">
                                            <FormLabel>Max rows</FormLabel>
                                            <FormInput type="number" control={testForm.control} name="maxRows" />
                                        </FormGroup>
                                    )}
                                    {rowSelector === "ByPrimaryKey" && (
                                        <FormStringValueList
                                            title="Primary key values"
                                            addButtonLabel="Add primary key value"
                                            control={testForm.control}
                                            name="primaryKeyValues"
                                            fieldNameAccessor={(idx) => `primaryKeyValues.${idx}.value`}
                                            defaultValue={{ value: "" }}
                                            className="mb-2"
                                        />
                                    )}
                                    <FormSwitch control={testForm.control} name="isSimulateOnDelete">
                                        Simulate on delete
                                    </FormSwitch>
                                </div>
                            </Accordion.Body>
                        </Accordion.Item>
                    </Accordion>
                    <TestResult asyncTest={asyncTest} />
                </ViewSheet.Body>
                <ViewSheet.Footer>
                    <ButtonWithSpinner
                        variant="info"
                        className="ms-auto rounded-pill"
                        onClick={handleSubmit}
                        isSpinning={asyncTest.loading}
                        icon="test"
                    >
                        Run test
                    </ButtonWithSpinner>
                </ViewSheet.Footer>
            </InnerForm>
        </ViewSheet>
    );
}

interface TestResultProps {
    asyncTest: UseAsyncReturn<Raven.Client.Documents.Operations.CdcSink.Test.TestCdcSinkMappingResult>;
}

function TestResult({ asyncTest }: TestResultProps) {
    if (asyncTest.status === "not-requested" || asyncTest.status === "loading") {
        return (
            <LazyLoad active className="h-100">
                <div className="h-100"></div>
            </LazyLoad>
        );
    }

    if (asyncTest.status === "error") {
        return <LoadError error="Failed to test table" />;
    }

    return (
        <>
            {asyncTest.result.Errors?.length > 0 && (
                <RichAlert variant="danger" className="small break-word">
                    <ExpandableListContainer items={asyncTest.result.Errors} renderItem={(err) => err} />
                </RichAlert>
            )}
            {asyncTest.result.Warnings?.length > 0 && (
                <RichAlert variant="warning" className="small break-word">
                    <ExpandableListContainer items={asyncTest.result.Warnings} renderItem={(warn) => warn} />
                </RichAlert>
            )}
            {asyncTest.result.Results?.map((result) => (
                <Code
                    key={result.DocumentId}
                    language="json"
                    code={JSON.stringify(JSON.parse(result.Document), null, 2)}
                />
            ))}
        </>
    );
}

const testSchema = yup.object({
    maxRows: yup.number().min(1).max(1000),
    rowSelector: yup.string<TestCdcSinkRowSelector>(),
    primaryKeyValues: yup
        .array()
        .of(yup.object({ value: yup.string() }))
        .when("rowSelector", {
            is: "ByPrimaryKey",
            then: (schema) => schema.min(1),
        }),
    isSimulateOnDelete: yup.boolean(),
});

type TestFormData = yup.InferType<typeof testSchema>;

const rowSelectorOptions: SelectOption<TestCdcSinkRowSelector>[] = [
    { value: "First", label: "First row" },
    { value: "ByPrimaryKey", label: "Primary Key" },
];
