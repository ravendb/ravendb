import "./SampleObjectAndSchemaFields.scss";
import { Icon } from "components/common/Icon";
import AceEditor, { AceEditorProps } from "../ace/AceEditor";
import ButtonWithSpinner from "../ButtonWithSpinner";
import { FormAceEditor, FormErrorIcon, FormSwitch, useErrorMessage } from "../Form";
import { Switch } from "../Checkbox";
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
import { AceEditorSamplesPanelConfig } from "../ace/AceEditorSamplesPanel";
import Badge from "react-bootstrap/Badge";
import { schemaFieldUtils } from "./schemaFieldUtils";

interface SampleObjectAndSchemaFieldsProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    setValue: UseFormSetValue<TFieldValues>;
    sampleObjectName: TName;
    sampleObjectLabel?: ReactNode;
    sampleObject: string;
    sampleObjectSyntaxHelp?: ReactNode;
    sampleObjectSamplesPanel?: AceEditorSamplesPanelConfig;
    sampleObjectTooltip?: ReactNode;
    sampleObjectPlaceholder?: string;
    jsonSchemaName: TName;
    jsonSchemaLabel?: ReactNode;
    jsonSchema: string;
    jsonSchemaSyntaxHelp?: ReactNode;
    jsonSchemaSamplesPanel?: AceEditorSamplesPanelConfig;
    jsonSchemaTooltip?: ReactNode;
    schemaType?: Raven.Server.Web.Studio.StudioTasksHandler.SchemaType;
    helpActionTooltipTitle?: string;
    canRegenerateSchemaName: TName;
    isStrictName?: TName;
    isStrict?: boolean;
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
    sampleObjectSamplesPanel,
    sampleObjectTooltip = <DefaultSampleObjectTooltip />,
    sampleObjectPlaceholder,
    jsonSchemaName,
    jsonSchemaLabel = "JSON schema",
    jsonSchema,
    jsonSchemaSyntaxHelp,
    jsonSchemaSamplesPanel,
    jsonSchemaTooltip = <DefaultJsonSchemaTooltip />,
    schemaType,
    helpActionTooltipTitle,
    canRegenerateSchemaName,
    isStrictName,
    isStrict,
}: SampleObjectAndSchemaFieldsProps<TFieldValues, TName>) {
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

    const { tasksService } = useServices();
    const sampleObjectValue = useWatch({ control, name: sampleObjectName });
    const { trigger, formState } = useFormContext();

    const sampleObjectError = useErrorMessage({ control, paths: [sampleObjectName] });
    const jsonSchemaError = useErrorMessage({ control, paths: [jsonSchemaName] });

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>(sampleObjectValue);

    const hasStrictMode = !!isStrictName;
    const hasWrittenSchema = schemaFieldUtils.getHasWrittenSchema(jsonSchema);
    const isSchemaGeneratedByServer = schemaFieldUtils.getIsSchemaGeneratedByServer(sampleObject, jsonSchema);
    const isWrittenSchemaStrict = hasStrictMode && schemaFieldUtils.getIsStrictToSave(jsonSchema, isStrict);

    const turnStrictModeOn = () => {
        if (hasStrictMode) {
            setValue(isStrictName, true as TFieldValues[TName], { shouldDirty: true });
        }
    };

    const asyncGenerateSchema = useAsyncCallback(async () => {
        try {
            const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(sampleObject), schemaType);
            setValue(jsonSchemaName, result.Result as TFieldValues[TName], {
                shouldValidate: true,
                shouldDirty: true,
            });
            turnStrictModeOn();
            setLastSampleObjectForGenerate(sampleObject);
        } catch (e) {
            console.error(e);
            messagePublisher.reportError("Failed to generate schema, please check the sample object");
        }
    });

    const canRegenerateSchema =
        schemaFieldUtils.getHasSampleObject(sampleObject) &&
        hasWrittenSchema &&
        lastSampleObjectForGenerate !== sampleObject;

    useEffect(() => {
        if (_.get(formState.dirtyFields, sampleObjectName)) {
            setValue(canRegenerateSchemaName, canRegenerateSchema as TFieldValues[TName], {
                shouldValidate: true,
            });
            trigger(jsonSchemaName);
        }
    }, [canRegenerateSchema]);

    const tabsId = useUniqueId("sample-object-and-schema-fields-tabs");

    const defaultActiveTab = hasWrittenSchema ? "json-schema" : "sample-object";

    const sampleObjectActions = buildEditorActions({
        onLoadFile: (value) =>
            setValue(sampleObjectName, value as TFieldValues[TName], { shouldValidate: true, shouldDirty: true }),
        syntaxHelp: sampleObjectSyntaxHelp,
        samplesPanel: sampleObjectSamplesPanel,
        helpTooltipTitle: helpActionTooltipTitle,
    });

    const jsonSchemaActions = buildEditorActions({
        onLoadFile: (value) =>
            setValue(jsonSchemaName, value as TFieldValues[TName], { shouldValidate: true, shouldDirty: true }),
        syntaxHelp: jsonSchemaSyntaxHelp,
        samplesPanel: jsonSchemaSamplesPanel,
    });

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
                            actions={sampleObjectActions}
                            samplesPanel={sampleObjectSamplesPanel}
                            isFullScreenLabelHidden
                        />
                        <div className="schema-footer">
                            {isSchemaGeneratedByServer && (
                                <div>
                                    <Icon icon="info" color="info" />
                                    The server will auto-generate a schema from this object
                                </div>
                            )}
                            {hasStrictMode && !hasWrittenSchema && <GeneratedSchemaStrictModeSwitch />}
                        </div>
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
                            {isWrittenSchemaStrict && (
                                <Badge bg="info" pill className="ms-1 text-uppercase">
                                    Strict
                                </Badge>
                            )}
                        </div>
                    }
                >
                    <div>
                        <FormAceEditor
                            aceRef={jsonSchemaRef}
                            control={control}
                            name={jsonSchemaName}
                            mode="json"
                            actions={jsonSchemaActions}
                            samplesPanel={jsonSchemaSamplesPanel}
                            isFullScreenLabelHidden
                            overlaySlot={
                                <>
                                    {isSchemaGeneratedByServer && (
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
                                </>
                            }
                        />
                        <div className="schema-footer">
                            {hasWrittenSchema && (
                                <div>
                                    <Icon icon="info" color="info" />
                                    {isWrittenSchemaStrict
                                        ? "This schema will be sent to the model in strict mode"
                                        : "This schema will be sent to the model"}
                                </div>
                            )}
                            {hasStrictMode && !hasWrittenSchema && <GeneratedSchemaStrictModeSwitch />}
                            {hasStrictMode && hasWrittenSchema && (
                                <div className="strict-mode-switch">
                                    <FormSwitch control={control} name={isStrictName}>
                                        Strict mode
                                    </FormSwitch>
                                    <StrictModeInfoIcon />
                                </div>
                            )}
                        </div>
                    </div>
                </Tab>
            </Tabs>
        </div>
    );
}

interface BuildEditorActionsArgs {
    onLoadFile: (value: string) => void;
    syntaxHelp?: ReactNode;
    samplesPanel?: AceEditorSamplesPanelConfig;
    helpTooltipTitle?: string;
}

function buildEditorActions({
    onLoadFile,
    syntaxHelp,
    samplesPanel,
    helpTooltipTitle,
}: BuildEditorActionsArgs): AceEditorProps["actions"] {
    return [
        { component: <AceEditor.FullScreenAction /> },
        { component: <AceEditor.FormatAction /> },
        { component: <AceEditor.LoadFileAction onLoad={onLoadFile} /> },
        ...(!samplesPanel && syntaxHelp
            ? [
                  {
                      component: <AceEditor.HelpAction message={syntaxHelp} tooltipTitle={helpTooltipTitle} />,
                      position: "bottom" as const,
                  },
              ]
            : []),
    ];
}

function GeneratedSchemaStrictModeSwitch() {
    return (
        <div className="strict-mode-switch is-locked">
            <Switch color="primary" selected disabled toggleSelection={() => {}}>
                Strict mode (always on for a generated schema)
            </Switch>
            <StrictModeInfoIcon />
        </div>
    );
}

function StrictModeInfoIcon() {
    return (
        <PopoverWithHoverWrapper message={<StrictModeTooltip />}>
            <Icon icon="info-new" margin="m-0" />
        </PopoverWithHoverWrapper>
    );
}

function StrictModeTooltip() {
    return (
        <>
            In strict mode the arguments that the model sends must match the schema exactly.
            <br />
            <br />
            The schema must follow the strict rules of the provider, otherwise the provider rejects the request. For
            example, OpenAI needs <code>&quot;additionalProperties&quot;: false</code> on every object and every
            property listed in <code>required</code>.
            <br />
            <br />A schema generated from the sample object is always sent in strict mode.
        </>
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
