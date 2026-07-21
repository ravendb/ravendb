import type { UseFormSetValue } from "react-hook-form";
import type { AiAgentConfiguration, AiAgentParameter, AiAgentToolQuery } from "@/api/generated/server-api";
import type {
    AgentConfigurationFormData,
    AgentFormData,
    AgentParameterFormData,
    AgentQueryToolFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

// Mapping between the wizard's editable agent configuration (form values) and the
// AiAgentConfiguration the server API speaks. Mirrors Studio's editAiAgentUtils.

export function emptyAgentConfiguration(): AgentConfigurationFormData {
    return {
        name: "",
        identifier: "",
        systemPrompt: "",
        sampleObject: "",
        outputSchema: "",
        parameters: [],
        queries: [],
    };
}

export function emptyAgentParameter(): AgentParameterFormData {
    return {
        name: "",
        type: "Default",
        description: "",
        policy: "Default",
        isSendToModel: true,
        isExpanded: true,
    };
}

export function emptyAgentQueryTool(): AgentQueryToolFormData {
    return {
        name: "",
        description: "",
        query: "",
        parametersSampleObject: "",
        parametersSchema: "",
        allowModelQueries: "Default",
        addToInitialContext: "Default",
        isExpanded: true,
    };
}

function toFormParameter(parameter: AiAgentParameter): AgentParameterFormData {
    return {
        name: parameter.name ?? "",
        type: parameter.type ?? "Default",
        description: parameter.description ?? "",
        policy: parameter.policy === "ForbidModelGeneration" ? "ForbidModelGeneration" : "Default",
        isSendToModel: parameter.sendToModel ?? true,
        isExpanded: false,
    };
}

function toTriState(value: boolean | null | undefined): AgentQueryToolFormData["allowModelQueries"] {
    if (value === true) {
        return "True";
    }
    if (value === false) {
        return "False";
    }
    return "Default";
}

function toOptionalBoolean(choice: AgentQueryToolFormData["allowModelQueries"]): boolean | null {
    if (choice === "True") {
        return true;
    }
    if (choice === "False") {
        return false;
    }
    return null;
}

function toFormQueryTool(query: AiAgentToolQuery): AgentQueryToolFormData {
    return {
        name: query.name ?? "",
        description: query.description ?? "",
        query: query.query ?? "",
        parametersSampleObject: query.parametersSampleObject ?? "",
        parametersSchema: query.parametersSchema ?? "",
        allowModelQueries: toTriState(query.options?.allowModelQueries),
        addToInitialContext: toTriState(query.options?.addToInitialContext),
        isExpanded: false,
    };
}

export function suggestionToAgentConfiguration(suggestion: AiAgentConfiguration): AgentConfigurationFormData {
    return {
        name: suggestion.name ?? "",
        identifier: suggestion.identifier ?? "",
        systemPrompt: suggestion.systemPrompt ?? "",
        sampleObject: suggestion.sampleObject ?? "",
        outputSchema: suggestion.outputSchema ?? "",
        parameters: (suggestion.parameters ?? []).map(toFormParameter),
        queries: (suggestion.queries ?? []).map(toFormQueryTool),
    };
}

// Used wherever the operator picks an AI-suggested candidate (connect step prefetch, the
// create step cards, and the review step's "AI suggestion" tab). Seeds the editable
// configuration, discarding any previous edits.
export function applySuggestionToForm(
    setValue: UseFormSetValue<AgentFormData>,
    suggestion: AiAgentConfiguration,
    index: number,
) {
    setValue("create.mode", "ai");
    setValue("create.selectedIndex", index);
    // Seeded configurations are expected to be valid, so validating here clears any
    // stale errors left over from an abandoned manual setup.
    setValue("review", suggestionToAgentConfiguration(suggestion), { shouldValidate: true });
}

// Builds the editable part of the provision payload from form values. Actions and
// sub-agents stay empty: the provision endpoint rejects them in this preview.
export function buildAgentConfigurationPayload(
    values: Pick<AgentFormData, "connection" | "review">,
): AiAgentConfiguration {
    const config = values.review;

    return {
        name: config.name.trim(),
        identifier: config.identifier.trim() || null,
        connectionStringName: values.connection.connectionStringName,
        systemPrompt: config.systemPrompt.trim(),
        sampleObject: config.sampleObject.trim() || null,
        outputSchema: config.outputSchema.trim() || null,
        parameters: config.parameters.map((parameter) => ({
            name: parameter.name.trim(),
            type: parameter.type,
            description: parameter.description.trim() || null,
            policy: parameter.policy,
            sendToModel: parameter.isSendToModel,
        })),
        queries: config.queries.map((tool) => {
            const allowModelQueries = toOptionalBoolean(tool.allowModelQueries);
            const addToInitialContext = toOptionalBoolean(tool.addToInitialContext);
            const parametersSampleObject = tool.parametersSampleObject.trim();
            const parametersSchema = tool.parametersSchema.trim();

            return {
                name: tool.name.trim(),
                description: tool.description.trim(),
                query: tool.query.trim(),
                // RavenDB requires a schema or a sample object per tool; "{}" means
                // "no parameters" (same default the suggest endpoint uses).
                parametersSampleObject: parametersSampleObject || (parametersSchema ? null : "{}"),
                parametersSchema: parametersSchema || null,
                options:
                    allowModelQueries === null && addToInitialContext === null
                        ? undefined
                        : { allowModelQueries, addToInitialContext },
            };
        }),
        actions: [],
        subAgents: [],
        disabled: false,
    };
}
