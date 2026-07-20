import { useEffect } from "react";
import { type FieldPath, useFormContext, useFormState, useWatch } from "react-hook-form";
import { CircleAlert } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import AceEditor from "@/components/ace-editor/ace-editor";

interface TabFieldConfig {
    name: FieldPath<AgentFormData>;
    label: string;
    placeholder: string;
    description: string;
}

export function SampleObjectAndSchemaTabs({
    sampleObject,
    schema,
}: {
    sampleObject: TabFieldConfig;
    schema: TabFieldConfig;
}) {
    const { control, getValues, getFieldState, trigger } = useFormContext<AgentFormData>();
    const formState = useFormState({ control, name: [sampleObject.name, schema.name] });
    const values = useWatch({ control, name: [sampleObject.name, schema.name] });

    const isSchemaFieldInvalid = getFieldState(schema.name, formState).invalid;
    const isEitherProvided = values.some((value) => typeof value === "string" && value.trim().length > 0);

    // The "either field must be provided" error lands on the schema field, so filling in the
    // sample object would not clear it on its own — revalidate the schema field explicitly.
    useEffect(() => {
        if (isSchemaFieldInvalid && isEitherProvided) {
            void trigger(schema.name);
        }
    }, [isSchemaFieldInvalid, isEitherProvided, trigger, schema.name]);

    // Match Studio: when a schema is already provided it takes precedence, so open on it.
    const defaultTab = getValues(schema.name) ? "schema" : "sample-object";

    return (
        <Tabs defaultValue={defaultTab}>
            <TabsList>
                <TabsTrigger value="sample-object">
                    {sampleObject.label}
                    {getFieldState(sampleObject.name, formState).invalid && (
                        <CircleAlert className="size-3.5 text-destructive" />
                    )}
                </TabsTrigger>
                <TabsTrigger value="schema">
                    {schema.label}
                    {getFieldState(schema.name, formState).invalid && (
                        <CircleAlert className="size-3.5 text-destructive" />
                    )}
                </TabsTrigger>
            </TabsList>
            <TabsContent value="sample-object">
                <SchemaTabEditor field={sampleObject} />
            </TabsContent>
            <TabsContent value="schema">
                <SchemaTabEditor field={schema} />
            </TabsContent>
        </Tabs>
    );
}

function SchemaTabEditor({ field }: { field: TabFieldConfig }) {
    const { control } = useFormContext<AgentFormData>();

    return (
        <FormAceEditor
            control={control}
            name={field.name}
            mode="json"
            label={field.label}
            labelClassName="sr-only"
            placeholder={field.placeholder}
            description={field.description}
            height="100px"
            actions={[
                { component: <AceEditor.FullScreenAction /> },
                { component: <AceEditor.FormatAction /> },
                { component: <AceEditor.AutoResizeHeightAction /> },
            ]}
        />
    );
}
