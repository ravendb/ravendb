import "./SampleObjectAndSchemaFields.scss";
import { Icon } from "components/common/Icon";
import AceEditor from "../ace/AceEditor";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { FormAceEditor, FormErrorIcon, useErrorMessage } from "../Form";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { Control, FieldPath, FieldValues, useFormContext, UseFormSetValue, useWatch } from "react-hook-form";
import ReactAce from "react-ace";
import { ReactNode, useEffect, useRef, useState } from "react";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import useUniqueId from "components/hooks/useUniqueId";
import classNames from "classnames";
import messagePublisher from "common/messagePublisher";

interface SampleObjectAndSchemaFieldsProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    setValue: UseFormSetValue<TFieldValues>;
    sampleObjectName: TName;
    sampleObjectLabel?: ReactNode;
    sampleObject: string;
    sampleObjectSyntaxHelp: React.ReactNode;
    sampleObjectTooltip?: ReactNode;
    sampleObjectPlaceholder?: string;
    jsonSchemaName: TName;
    jsonSchemaLabel?: ReactNode;
    jsonSchema: string;
    jsonSchemaSyntaxHelp: React.ReactNode;
    jsonSchemaTooltip?: ReactNode;
    schemaType?: Raven.Server.Web.Studio.StudioTasksHandler.SchemaType;
    helpActionTooltipTitle?: string;
    canRegenerateSchemaName: TName;
}

export default function SampleObjectAndSchemaFields<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({
    control,
    setValue,
    sampleObjectName,
    sampleObjectLabel = "Sample response object",
    sampleObject,
    sampleObjectSyntaxHelp,
    sampleObjectTooltip = <DefaultSampleObjectTooltip />,
    sampleObjectPlaceholder,
    jsonSchemaName,
    jsonSchemaLabel = "JSON schema",
    jsonSchema,
    jsonSchemaSyntaxHelp,
    jsonSchemaTooltip = <DefaultJsonSchemaTooltip />,
    schemaType,
    helpActionTooltipTitle,
    canRegenerateSchemaName,
}: SampleObjectAndSchemaFieldsProps<TFieldValues, TName>) {
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

    const { tasksService } = useServices();
    const sampleObjectValue = useWatch({ control, name: sampleObjectName });
    const { trigger, formState } = useFormContext();

    const sampleObjectError = useErrorMessage({ control, paths: [sampleObjectName] });
    const jsonSchemaError = useErrorMessage({ control, paths: [jsonSchemaName] });

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>(sampleObjectValue);

    const asyncGenerateSchema = useAsyncCallback(async () => {
        try {
            const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(sampleObject), schemaType);
            setValue(jsonSchemaName, result.Result as TFieldValues[TName], { shouldValidate: true });
            setLastSampleObjectForGenerate(sampleObject);
        } catch (e) {
            console.error(e);
            messagePublisher.reportError("Failed to generate schema, please check the sample object");
        }
    });

    const canRegenerateSchema = !!sampleObject && !!jsonSchema && lastSampleObjectForGenerate !== sampleObject;

    useEffect(() => {
        if (_.get(formState.dirtyFields, sampleObjectName)) {
            setValue(canRegenerateSchemaName, canRegenerateSchema as TFieldValues[TName], {
                shouldValidate: true,
            });
            trigger(jsonSchemaName);
        }
    }, [canRegenerateSchema]);

    const tabsId = useUniqueId("sample-object-and-schema-fields-tabs");

    const defaultActiveTab = jsonSchema ? "json-schema" : "sample-object";

    return (
        <div className="sample-object-and-schema-tabs">
            <Tabs defaultActiveKey={defaultActiveTab} id={tabsId}>
                <Tab
                    eventKey="sample-object"
                    title={
                        <div className="hstack">
                            <div className={classNames({ "text-danger": sampleObjectError.hasErrors })}>
                                {sampleObjectLabel}
                            </div>
                            <PopoverWithHoverWrapper message={sampleObjectTooltip}>
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                            <FormErrorIcon control={control} paths={[sampleObjectName]} onError={() => {}} />
                        </div>
                    }
                >
                    <div>
                        <FormAceEditor
                            aceRef={sampleObjectRef}
                            control={control}
                            name={sampleObjectName}
                            mode="json"
                            placeholder={sampleObjectPlaceholder}
                            actions={[
                                { component: <AceEditor.FullScreenAction /> },
                                { component: <AceEditor.FormatAction /> },
                                {
                                    component: (
                                        <AceEditor.LoadFileAction
                                            onLoad={(value) =>
                                                setValue(sampleObjectName, value as TFieldValues[TName], {
                                                    shouldValidate: true,
                                                })
                                            }
                                        />
                                    ),
                                },
                                {
                                    component: (
                                        <AceEditor.HelpAction
                                            message={sampleObjectSyntaxHelp}
                                            tooltipTitle={helpActionTooltipTitle}
                                        />
                                    ),
                                    position: "bottom",
                                },
                            ]}
                            isFullScreenLabelHidden
                        />
                        {!!sampleObject && !jsonSchema && (
                            <div className="mt-2">
                                <Icon icon="info" color="info" />
                                The server will auto-generate a schema from this object
                            </div>
                        )}
                    </div>
                </Tab>
                <Tab
                    eventKey="json-schema"
                    title={
                        <div className="hstack">
                            <span className={classNames(jsonSchemaError.hasErrors && "text-danger")}>
                                {jsonSchemaLabel}
                            </span>
                            <PopoverWithHoverWrapper message={jsonSchemaTooltip}>
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                            <FormErrorIcon control={control} paths={[jsonSchemaName]} />
                        </div>
                    }
                >
                    <div>
                        <div className="position-relative">
                            <FormAceEditor
                                aceRef={jsonSchemaRef}
                                control={control}
                                name={jsonSchemaName}
                                mode="json"
                                actions={[
                                    { component: <AceEditor.FullScreenAction /> },
                                    { component: <AceEditor.FormatAction /> },
                                    {
                                        component: (
                                            <AceEditor.LoadFileAction
                                                onLoad={(value) =>
                                                    setValue(jsonSchemaName, value as TFieldValues[TName], {
                                                        shouldValidate: true,
                                                    })
                                                }
                                            />
                                        ),
                                    },
                                    {
                                        component: <AceEditor.HelpAction message={jsonSchemaSyntaxHelp} />,
                                        position: "bottom",
                                    },
                                ]}
                                isFullScreenLabelHidden
                            />
                            {!!sampleObject && !jsonSchema && (
                                <ButtonWithSpinner
                                    className="rounded-pill position-absolute top-50 start-50 translate-middle"
                                    variant="primary"
                                    onClick={asyncGenerateSchema.execute}
                                    isSpinning={asyncGenerateSchema.loading}
                                    title="Click to view and edit the schema generated by the server"
                                >
                                    View schema
                                </ButtonWithSpinner>
                            )}
                            {canRegenerateSchema && (
                                <ButtonWithSpinner
                                    className="rounded-pill position-absolute z-1"
                                    style={{
                                        top: "150px",
                                        right: "54px",
                                    }}
                                    variant="primary"
                                    onClick={asyncGenerateSchema.execute}
                                    isSpinning={asyncGenerateSchema.loading}
                                    icon="refresh"
                                    title="Regenerate the schema from the sample response object"
                                >
                                    Regenerate schema
                                </ButtonWithSpinner>
                            )}
                        </div>
                        {!!jsonSchema && (
                            <div className="mt-2">
                                <Icon icon="info" color="info" />
                                This schema will be sent to the model
                            </div>
                        )}
                    </div>
                </Tab>
            </Tabs>
        </div>
    );
}

function DefaultSampleObjectTooltip() {
    return (
        <>
            This object defines the structure of the output you expect from the model. It is not sent to the model.
            <br />
            RavenDB will use it to generate a <strong>JSON schema</strong>, which will be included in the request to the
            model.
        </>
    );
}

function DefaultJsonSchemaTooltip() {
    return (
        <>
            The JSON schema defines the structure and types of the output you expect from the model.
            <br />
            This schema is included in the request to the model.
            <br />
            <br />
            If you don&apos;t provide a schema, RavenDB will generate one automatically based on the sample response
            object.
            <br />
            <br />
            If you provide both a sample object and a schema, the schema takes precedence and will be sent to the model.
        </>
    );
}
