import React from "react";
import { FormProvider, useForm } from "react-hook-form";
import { rtlRender } from "test/rtlTestUtils";
import SampleObjectAndSchemaFields from "./SampleObjectAndSchemaFields";

interface FormData {
    sampleObject: string;
    jsonSchema: string;
    canRegenerateSchema: boolean;
}

function SampleObjectAndSchemaFieldsWithForm() {
    const form = useForm<FormData>({
        defaultValues: { sampleObject: '{ "name": "" }', jsonSchema: "", canRegenerateSchema: false },
    });
    const { sampleObject, jsonSchema } = form.watch();

    return (
        <FormProvider {...form}>
            <SampleObjectAndSchemaFields
                control={form.control}
                setValue={form.setValue}
                sampleObjectName="sampleObject"
                sampleObject={sampleObject}
                jsonSchemaName="jsonSchema"
                jsonSchema={jsonSchema}
                jsonSchemaSamplesPanel={{
                    tabs: [{ key: "schema", label: "Sample schema", icon: "document", content: () => null }],
                }}
                canRegenerateSchemaName="canRegenerateSchema"
            />
        </FormProvider>
    );
}

describe("SampleObjectAndSchemaFields", () => {
    it("renders the view schema button inside the JSON schema editor so the samples panel does not shift it", async () => {
        const { screen, fireClick } = rtlRender(<SampleObjectAndSchemaFieldsWithForm />);

        await fireClick(screen.getByText("JSON schema"));

        expect(screen.getByRole("button", { name: /View schema/ }).closest(".ace-editor")).not.toBeNull();
    });
});
