import type { UseFormSetValue } from "react-hook-form";
import type {
    AiAgentConfiguration,
    AiAgentParameter,
    AiAgentToolAction,
    AiAgentToolQuery,
    WebhookBinding,
} from "@/api/generated/server-api";
import type {
    AgentActionFormData,
    AgentConfigurationFormData,
    AgentFormData,
    AgentParameterFormData,
    AgentQueryToolFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

// Mapping between the wizard's editable agent configuration (form values) and the
// AiAgentConfiguration the server API speaks. Mirrors Studio's editAiAgentUtils.

// The app's chat UI only understands a single "reply" string response, so the response
// shape editor is hidden for now and every agent uses this fixed sample object.
export const AGENT_SAMPLE_OBJECT = '{"reply":""}';

export function emptyAgentConfiguration(): AgentConfigurationFormData {
    return {
        name: "",
        identifier: "",
        systemPrompt: "",
        sampleObject: AGENT_SAMPLE_OBJECT,
        outputSchema: "",
        parameters: [],
        queries: [],
        actions: [],
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

export function emptyAgentAction(): AgentActionFormData {
    return {
        name: "",
        description: "",
        parametersSampleObject: "",
        parametersSchema: "",
        url: "",
        secret: "",
        maxResponseSize: null,
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

function toFormAction(action: AiAgentToolAction, binding: WebhookBinding | undefined): AgentActionFormData {
    return {
        name: action.name ?? "",
        description: action.description ?? "",
        parametersSampleObject: action.parametersSampleObject ?? "",
        parametersSchema: action.parametersSchema ?? "",
        url: binding?.url ?? "",
        secret: binding?.secret ?? "",
        maxResponseSize: binding?.maxResponseSize ?? null,
        isExpanded: false,
    };
}

// Actions are mapped only when their bindings come with them, which is the case for a stored agent
// (agents.get returns both) but never for an AI suggestion. A suggested action has no webhook, so
// seeding it would put a row with an empty URL in front of an operator who never asked for one.
export function suggestionToAgentConfiguration(
    suggestion: AiAgentConfiguration,
    actionBindings?: Record<string, WebhookBinding>,
): AgentConfigurationFormData {
    // action names are matched without case everywhere on the server, so match the sidecar's keys the same way
    const bindingsByName = new Map(
        Object.entries(actionBindings ?? {}).map(([name, binding]) => [name.toLowerCase(), binding]),
    );

    return {
        name: suggestion.name ?? "",
        identifier: suggestion.identifier ?? "",
        systemPrompt: suggestion.systemPrompt ?? "",
        // The response shape is not editable while the editor is hidden, so any suggested or
        // stored shape is replaced with the fixed one.
        sampleObject: AGENT_SAMPLE_OBJECT,
        outputSchema: "",
        parameters: (suggestion.parameters ?? []).map(toFormParameter),
        queries: (suggestion.queries ?? []).map(toFormQueryTool),
        actions: actionBindings
            ? (suggestion.actions ?? []).map((action) =>
                  toFormAction(action, bindingsByName.get((action.name ?? "").toLowerCase())),
              )
            : [],
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

// Builds the editable part of the provision payload from form values. Sub-agents stay
// empty: the provision endpoint rejects them in this preview.
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
        actions: config.actions.map((action) => {
            const parametersSampleObject = action.parametersSampleObject.trim();
            const parametersSchema = action.parametersSchema.trim();

            return {
                name: action.name.trim(),
                description: action.description.trim(),
                // same rule as the query tools above: "{}" stands for "no parameters"
                parametersSampleObject: parametersSampleObject || (parametersSchema ? null : "{}"),
                parametersSchema: parametersSchema || null,
            };
        }),
        subAgents: [],
        disabled: false,
    };
}

// The other half of every action row. Keyed by action name, which is what the server matches
// bindings on, so the 1:1 mapping it validates holds by construction.
export function buildActionBindings(values: Pick<AgentFormData, "review">): Record<string, WebhookBinding> {
    return Object.fromEntries(
        values.review.actions.map((action) => [
            action.name.trim(),
            {
                url: action.url.trim(),
                secret: action.secret.trim() || null,
                maxResponseSize: action.maxResponseSize,
            },
        ]),
    );
}
