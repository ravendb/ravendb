import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import AceEditor from "../ace/AceEditor";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { FormAceEditor, FormGroup, FormLabel } from "../Form";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { Control, FieldPath, FieldValues, useFormContext, UseFormSetValue, useWatch } from "react-hook-form";
import ReactAce from "react-ace";
import { ReactNode, useEffect, useRef, useState } from "react";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";

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
    sampleObjectTooltip,
    sampleObjectPlaceholder,
    jsonSchemaName,
    jsonSchemaLabel = "JSON schema",
    jsonSchema,
    jsonSchemaSyntaxHelp,
    jsonSchemaTooltip,
    schemaType,
    helpActionTooltipTitle,
    canRegenerateSchemaName,
}: SampleObjectAndSchemaFieldsProps<TFieldValues, TName>) {
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

    const { tasksService } = useServices();
    const sampleObjectValue = useWatch({ control, name: sampleObjectName });
    const { trigger, formState } = useFormContext();

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>(sampleObjectValue);

    const asyncGenerateSchema = useAsyncCallback(async () => {
        const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(sampleObject), schemaType);
        setValue(jsonSchemaName, result.Result as TFieldValues[TName], { shouldValidate: true });
        setLastSampleObjectForGenerate(sampleObject);
    });

    const canRegenerateSchema = !!sampleObject && !!jsonSchema && lastSampleObjectForGenerate !== sampleObject;

    useEffect(() => {
        if (formState.dirtyFields[sampleObjectName]) {
            setValue(canRegenerateSchemaName, canRegenerateSchema as TFieldValues[TName], {
                shouldValidate: true,
            });
            trigger(jsonSchemaName);
        }
    }, [canRegenerateSchema]);

    return (
        <div>
            <FormGroup className="vstack">
                <FormLabel className="hstack justify-content-between align-items-start">
                    <div>
                        {sampleObjectLabel}
                        <PopoverWithHoverWrapper
                            message={
                                sampleObjectTooltip ?? (
                                    <>
                                        This object defines the structure of the output you expect from the model. It is
                                        not sent to the model.
                                        <br />
                                        RavenDB will use it to generate a <strong>JSON schema</strong>, which will be
                                        included in the request to the model.
                                    </>
                                )
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </div>
                    {!!sampleObject && !jsonSchema && (
                        <Badge pill bg="info" style={{ whiteSpace: "normal" }}>
                            The server will auto-generate a schema from this object
                        </Badge>
                    )}
                </FormLabel>
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
                />
            </FormGroup>
            <FormGroup className="vstack">
                <FormLabel className="flex-grow-1 hstack justify-content-between align-items-start">
                    <div>
                        {jsonSchemaLabel}
                        <PopoverWithHoverWrapper
                            message={
                                jsonSchemaTooltip ?? (
                                    <>
                                        The JSON schema defines the structure and types of the output you expect from
                                        the model.
                                        <br />
                                        This schema is included in the request to the model.
                                        <br />
                                        <br />
                                        If you don&apos;t provide a schema, RavenDB will generate one automatically
                                        based on the sample response object.
                                        <br />
                                        <br />
                                        If you provide both a sample object and a schema, the schema takes precedence
                                        and will be sent to the model.
                                    </>
                                )
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </div>
                    {!!jsonSchema && (
                        <Badge pill bg="info" style={{ whiteSpace: "normal" }}>
                            This schema will be sent to the model
                        </Badge>
                    )}
                </FormLabel>
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
            </FormGroup>
        </div>
    );
}
