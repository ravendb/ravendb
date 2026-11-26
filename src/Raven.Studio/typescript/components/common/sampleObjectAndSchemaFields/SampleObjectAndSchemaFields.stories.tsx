import { Meta, StoryObj } from "@storybook/react-webpack5";
import { FormProvider, useForm } from "react-hook-form";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import {
    queryToolParametersSamplesTabs,
    queryToolParametersSchemaSamplesTabs,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editGenAiTask/editGenAiTaskSamplesData";
import SampleObjectAndSchemaFields from "./SampleObjectAndSchemaFields";

interface FormData {
    sampleObject: string;
    jsonSchema: string;
    canRegenerateSchema: boolean;
    isStrict: boolean;
}

interface StoryArgs {
    hasStrictMode: boolean;
    isStrict: boolean;
    hasSampleObject: boolean;
    hasJsonSchema: boolean;
    hasSamplesPanel: boolean;
    hasLongValues: boolean;
    isNarrow: boolean;
}

export default {
    title: "Common/SampleObjectAndSchemaFields",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args) => <SampleObjectAndSchemaFieldsWithForm {...args} />,
    args: {
        hasStrictMode: true,
        isStrict: true,
        hasSampleObject: true,
        hasJsonSchema: true,
        hasSamplesPanel: false,
        hasLongValues: false,
        isNarrow: false,
    },
} satisfies Meta<StoryArgs>;

type Story = StoryObj<StoryArgs>;

export const Default: Story = {};

export const WithoutStrictMode: Story = {
    args: { hasStrictMode: false },
};

export const StrictModeOn: Story = {};

export const StrictModeOff: Story = {
    args: { isStrict: false },
};

export const GeneratedSchema: Story = {
    args: { hasJsonSchema: false },
};

export const GeneratedSchemaWithSamplesPanel: Story = {
    args: { hasJsonSchema: false, hasSamplesPanel: true },
};

export const Empty: Story = {
    args: { hasSampleObject: false, hasJsonSchema: false },
};

export const LongValues: Story = {
    args: { hasLongValues: true },
};

export const Narrow: Story = {
    args: { isNarrow: true },
};

function SampleObjectAndSchemaFieldsWithForm({
    hasStrictMode,
    isStrict,
    hasSampleObject,
    hasJsonSchema,
    hasSamplesPanel,
    hasLongValues,
    isNarrow,
}: StoryArgs) {
    const { tasksService } = mockServices;
    tasksService.withJsonSchemaFromSampleObject();

    const form = useForm<FormData>({
        defaultValues: {
            sampleObject: hasSampleObject ? getSampleObject(hasLongValues) : "",
            jsonSchema: hasJsonSchema ? getJsonSchema(hasLongValues) : "",
            canRegenerateSchema: false,
            isStrict,
        },
    });

    const formValues = form.watch();

    return (
        <FormProvider {...form}>
            <div
                className="ai-agents-config well p-2 rounded-2 border border-secondary"
                style={{ maxWidth: isNarrow ? NARROW_WIDTH_IN_PX : undefined }}
            >
                <SampleObjectAndSchemaFields
                    control={form.control}
                    setValue={form.setValue}
                    sampleObjectName="sampleObject"
                    sampleObjectLabel="Sample parameters object"
                    sampleObject={formValues.sampleObject}
                    sampleObjectSamplesPanel={hasSamplesPanel ? { tabs: queryToolParametersSamplesTabs } : undefined}
                    jsonSchemaName="jsonSchema"
                    jsonSchemaLabel="Parameters JSON schema"
                    jsonSchema={formValues.jsonSchema}
                    jsonSchemaSamplesPanel={
                        hasSamplesPanel ? { tabs: queryToolParametersSchemaSamplesTabs } : undefined
                    }
                    schemaType="ToolParameters"
                    canRegenerateSchemaName="canRegenerateSchema"
                    isStrictName={hasStrictMode ? "isStrict" : undefined}
                    isStrict={formValues.isStrict}
                />
            </div>
        </FormProvider>
    );
}

const NARROW_WIDTH_IN_PX = 360;

const LONG_PROPERTY_NAME = "theIdOfTheOrderThatTheCustomerComplainsAboutInTheCurrentConversationWithTheSupportTeam";

function getSampleObject(hasLongValues: boolean): string {
    const propertyName = hasLongValues ? LONG_PROPERTY_NAME : "orderId";

    return JSON.stringify(
        {
            [propertyName]: "The ID of the order the customer complains about",
            problem: "Describe the problem",
        },
        null,
        4
    );
}

function getJsonSchema(hasLongValues: boolean): string {
    const jsonSchema = TasksStubs.getJsonSchemaFromSampleObject();

    return hasLongValues ? jsonSchema.replaceAll("orderId", LONG_PROPERTY_NAME) : jsonSchema;
}
