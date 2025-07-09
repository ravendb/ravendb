import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import AceEditor from "../ace/AceEditor";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { FormAceEditor, FormGroup, FormLabel } from "../Form";
import PopoverWithHoverWrapper from "../PopoverWithHoverWrapper";
import { Control, FieldPath, FieldValues, UseFormSetValue } from "react-hook-form";
import ReactAce from "react-ace";
import { useRef, useState } from "react";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";

interface SampleObjectAndSchemaFieldsProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    setValue: UseFormSetValue<TFieldValues>;
    sampleObjectName: TName;
    sampleObject: string;
    sampleObjectSyntaxHelp: React.ReactNode;
    jsonSchemaName: TName;
    jsonSchema: string;
    jsonSchemaSyntaxHelp: React.ReactNode;
}

export default function SampleObjectAndSchemaFields<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
>({
    control,
    setValue,
    sampleObjectName,
    sampleObject,
    sampleObjectSyntaxHelp,
    jsonSchemaName,
    jsonSchema,
    jsonSchemaSyntaxHelp,
}: SampleObjectAndSchemaFieldsProps<TFieldValues, TName>) {
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

    const { tasksService } = useServices();

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>("");

    const asyncGenerateSchema = useAsyncCallback(async () => {
        const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(sampleObject));
        setValue(jsonSchemaName, result.Result as TFieldValues[TName], { shouldValidate: true });
        setLastSampleObjectForGenerate(sampleObject);
    });

    const canRegenerateSchema = !!sampleObject && !!jsonSchema && lastSampleObjectForGenerate !== sampleObject;

    return (
        <div>
            <div className="hstack gap-1">
                <div className="vstack w-50">
                    <FormGroup className="vstack">
                        <FormLabel className="hstack justify-content-between align-items-start">
                            <div>
                                Sample response object
                                <PopoverWithHoverWrapper
                                    message={
                                        <>
                                            This object defines the structure of the output you expect from the model.
                                            It is not sent to the model.
                                            <br />
                                            RavenDB will use it to generate a <strong>JSON schema</strong>, which will
                                            be included in the request to the model.
                                        </>
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
                                    component: <AceEditor.HelpAction message={sampleObjectSyntaxHelp} />,
                                    position: "bottom",
                                },
                            ]}
                        />
                    </FormGroup>
                </div>
                <div className="vstack w-50">
                    <FormGroup className="vstack">
                        <FormLabel className="flex-grow-1 hstack justify-content-between align-items-start">
                            <div>
                                JSON schema
                                <PopoverWithHoverWrapper
                                    message={
                                        <>
                                            The JSON schema defines the structure and types of the output you expect
                                            from the model.
                                            <br />
                                            This schema is included in the request to the model.
                                            <br />
                                            <br />
                                            If you don&apos;t provide a schema, RavenDB will generate one automatically
                                            based on the sample response object.
                                            <br />
                                            If you provide both a sample object and a schema, the schema takes
                                            precedence and will be sent to the model.
                                        </>
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
                                        bottom: "20px",
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
            </div>
        </div>
    );
}
