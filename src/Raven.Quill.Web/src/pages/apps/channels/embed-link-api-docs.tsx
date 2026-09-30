import { useState } from "react";
import { Text } from "@/components/typography";
import { ShieldAlertIcon } from "lucide-react";
import { CodeBlockTabs } from "@/components/data/code-block-tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { originForSubdomain } from "@/lib/subdomain-origin";
import { readStoredValue, writeStoredValue } from "@/lib/safe-storage";
import {
    buildMintEmbedLinkUrl,
    DEFAULT_MAX_INVOCATIONS,
    DEFAULT_TTL_SECONDS,
    MAX_INVOCATIONS,
    MAX_TTL_SECONDS,
    MIN_INVOCATIONS,
    MIN_TTL_SECONDS,
} from "@/pages/apps/channels/embed-link-utils";
import { buildBackedHostPageSnippet } from "@/pages/apps/channels/embed-host-page-snippets";
import { InlineCode } from "@/components/data/inline-code";
import { NumberedSteps } from "@/components/data/numbered-steps";
import { SectionCard } from "@/pages/apps/section-card";
import type { HighlightLanguage } from "@/components/ace-editor/static-highlight";
import type { AgentParameterSummary } from "@/api/generated/server-api";
import {
    snippetLiteralFor,
    snippetValueFor,
    typeLabelFor,
    type SnippetSyntax,
} from "@/pages/apps/channels/agent-parameter-values";

const LANGUAGE_STORAGE_KEY = "quill-embed-api-docs-language";

// The host page snippet grows to its natural height, but never shrinks below this so the block still
// reads as a code viewport.
const HOST_PAGE_MIN_LINES = 16;

type Language = "bash" | "powershell" | "csharp" | "python" | "node";

type LanguageOption = {
    value: Language;
    label: string;
    mode: HighlightLanguage;
};

const LANGUAGE_OPTIONS: LanguageOption[] = [
    { value: "bash", label: "cURL", mode: "sh" },
    { value: "powershell", label: "PowerShell", mode: "powershell" },
    { value: "csharp", label: "C#", mode: "csharp" },
    { value: "python", label: "Python", mode: "python" },
    { value: "node", label: "Node.js", mode: "javascript" },
];

function readLanguage(): Language {
    const stored = readStoredValue(LANGUAGE_STORAGE_KEY);
    return LANGUAGE_OPTIONS.some((language) => language.value === stored) ? (stored as Language) : "bash";
}

type EmbedLinkApiDocsProps = {
    slug: string;
    channelId: string;
    parameters: AgentParameterSummary[];
};

export function EmbedLinkApiDocs({ slug, channelId, parameters }: EmbedLinkApiDocsProps) {
    const hasParameters = parameters.length > 0;
    const requests = buildRequestSnippets(slug, channelId, parameters);
    const embedOrigin = originForSubdomain("public");

    const [language, setLanguage] = useState<Language>(readLanguage);

    const onLanguageChange = (value: string) => {
        const next = value as Language;
        writeStoredValue(LANGUAGE_STORAGE_KEY, next);
        setLanguage(next);
    };

    const fields = [
        { name: "channelId", description: "The web widget channel the link is minted for (already filled in)." },
        {
            name: "ttlSeconds",
            description: `Link lifetime in seconds, ${MIN_TTL_SECONDS}–${MAX_TTL_SECONDS.toLocaleString()} (default ${DEFAULT_TTL_SECONDS.toLocaleString()}).`,
        },
        {
            name: "maxInvocations",
            description: `Chats allowed before the link stops, ${MIN_INVOCATIONS}–${MAX_INVOCATIONS.toLocaleString()} (default ${DEFAULT_MAX_INVOCATIONS}).`,
        },
        ...(hasParameters
            ? [
                  {
                      name: "parameters",
                      description: `Values bound into the link for this agent (${describeParameters(parameters)}); each must match its declared type, and omitting a required one returns 400.`,
                  },
              ]
            : []),
    ];

    return (
        <SectionCard title="Embed on your own site" isRaised>
            <div className="mt-4">
                <NumberedSteps
                    steps={[
                        {
                            title: "Mint links from your backend",
                            content: (
                                <>
                                    <Text variant="muted" className="max-w-prose">
                                        Your server POSTs to the embed-links endpoint with your Dashboard API key in the{" "}
                                        <InlineCode>X-Api-Key</InlineCode> header, then hands the page nothing but the
                                        returned <InlineCode>url</InlineCode>. The app and channel are already filled in
                                        below - swap in your <InlineCode>QUILL_API_KEY</InlineCode>
                                        {hasParameters ? " and the parameter values" : ""}.
                                    </Text>

                                    <div className="mt-3 grid gap-4 lg:grid-cols-2">
                                        <CodeBlockTabs
                                            value={language}
                                            onValueChange={onLanguageChange}
                                            copyLabel="Copy server-side mint request"
                                            tabs={LANGUAGE_OPTIONS.map(({ value, label, mode }) => ({
                                                value,
                                                label,
                                                language: mode,
                                                code: requests[value],
                                            }))}
                                        />
                                        <ParametersPanel fields={fields} />
                                    </div>

                                    <Alert variant="warning" className="mt-4">
                                        <ShieldAlertIcon />
                                        <AlertTitle>Run this on your server, never in a browser</AlertTitle>
                                        <AlertDescription>
                                            The Dashboard API key grants full access to every app, not just this widget,
                                            so it must never be shipped to a page or called from client-side JavaScript.
                                            <br /> The endpoint also sends no CORS headers, so a browser{" "}
                                            <InlineCode>fetch</InlineCode> to it fails on preflight regardless.
                                        </AlertDescription>
                                    </Alert>
                                </>
                            ),
                        },
                        {
                            title: "Show the link in your page",
                            content: (
                                <>
                                    <Text variant="muted" className="max-w-prose">
                                        Then in the page, point an iframe at the <InlineCode>url</InlineCode> your
                                        endpoint returned:
                                    </Text>
                                    <CodeBlockTabs
                                        value="html"
                                        copyLabel="Copy host page"
                                        minLines={HOST_PAGE_MIN_LINES}
                                        className="mt-3"
                                        tabs={[
                                            {
                                                value: "html",
                                                label: "Host page",
                                                language: "html",
                                                code: buildBackedHostPageSnippet(embedOrigin),
                                            },
                                        ]}
                                    />
                                </>
                            ),
                        },
                    ]}
                />
            </div>
        </SectionCard>
    );
}

// The request parameters, shown as an always-expanded panel beside the code block so the user keeps a
// quick reference in view. Mirrors the code card's header/border so the two-column row stays balanced.
//
// On lg the two columns share a grid row, so the code block sets the row height. The panel absolutely
// fills that height (its own content no longer grows the row) and the list scrolls when it overflows —
// so a long parameter list can never make this column taller than the code block. Below lg the columns
// stack and the panel takes its natural height.
function ParametersPanel({ fields }: { fields: { name: string; description: string }[] }) {
    return (
        <div className="min-w-0 lg:relative">
            <div className="flex flex-col overflow-hidden rounded-lg border lg:absolute lg:inset-0">
                <div className="border-b bg-muted/50 px-3 py-2.5 text-xs font-medium">Parameters</div>
                <dl className="min-h-0 flex-1 divide-y divide-border overflow-y-auto text-sm">
                    {fields.map((field) => (
                        <div
                            key={field.name}
                            className="grid items-baseline gap-x-3 px-3 py-2.5 sm:grid-cols-[8rem_1fr]"
                        >
                            <dt className="font-mono text-xs font-medium text-muted-foreground">{field.name}</dt>
                            <dd className="text-muted-foreground">{field.description}</dd>
                        </div>
                    ))}
                </dl>
            </div>
        </div>
    );
}

const API_KEY_PLACEHOLDER = "<your QUILL_API_KEY>";

function buildRequestSnippets(
    slug: string,
    channelId: string,
    parameters: AgentParameterSummary[],
): Record<Language, string> {
    const url = buildMintEmbedLinkUrl(slug);
    const hasParameters = parameters.length > 0;

    return {
        bash: buildCurlSnippet(url, channelId, parameters, "curl", "\\"),
        powershell: buildCurlSnippet(url, channelId, parameters, "curl.exe", "`"),
        csharp: buildCSharpSnippet(url, channelId, parameters, hasParameters),
        python: buildPythonSnippet(url, channelId, parameters, hasParameters),
        node: buildNodeSnippet(url, channelId, parameters, hasParameters),
    };
}

function describeParameters(parameters: AgentParameterSummary[]): string {
    return parameters
        .map((parameter) => {
            const typeLabel = typeLabelFor(parameter.type);
            return typeLabel ? `${parameter.name}: ${typeLabel}` : parameter.name;
        })
        .join(", ");
}

function snippetEntries(parameters: AgentParameterSummary[]): Record<string, unknown> {
    return Object.fromEntries(parameters.map((parameter) => [parameter.name, snippetValueFor(parameter.type)]));
}

function inlineSnippetEntries(parameters: AgentParameterSummary[], syntax: SnippetSyntax): string {
    return parameters
        .map(
            (parameter) =>
                `${JSON.stringify(parameter.name)}: ${snippetLiteralFor(syntax, snippetValueFor(parameter.type))}`,
        )
        .join(", ");
}

// bash continues lines with "\", PowerShell with a backtick; PowerShell also needs curl.exe so
// it doesn't resolve to the Invoke-WebRequest alias on Windows PowerShell 5.1.
function buildCurlSnippet(
    url: string,
    channelId: string,
    parameters: AgentParameterSummary[],
    curl: string,
    continuation: string,
) {
    const body: Record<string, unknown> = {
        channelId,
        ttlSeconds: DEFAULT_TTL_SECONDS,
        maxInvocations: DEFAULT_MAX_INVOCATIONS,
    };
    if (parameters.length > 0) {
        body.parameters = snippetEntries(parameters);
    }

    const indentedBody = JSON.stringify(body, null, 2)
        .split("\n")
        .map((line, index) => (index === 0 ? line : `  ${line}`))
        .join("\n");

    return [
        `${curl} -X POST ${continuation}`,
        `  "${url}" ${continuation}`,
        `  -H "X-Api-Key: ${API_KEY_PLACEHOLDER}" ${continuation}`,
        `  -H "Content-Type: application/json" ${continuation}`,
        `  -d '${indentedBody}'`,
    ].join("\n");
}

function buildCSharpSnippet(
    url: string,
    channelId: string,
    parameters: AgentParameterSummary[],
    hasParameters: boolean,
) {
    const parameterEntries = parameters
        .map(
            (parameter) =>
                `[${JSON.stringify(parameter.name)}] = ${snippetLiteralFor("csharp", snippetValueFor(parameter.type))}`,
        )
        .join(", ");

    return [
        "using System.Net.Http.Json;",
        "using System.Text.Json;",
        "",
        "using var client = new HttpClient();",
        `client.DefaultRequestHeaders.Add("X-Api-Key", "${API_KEY_PLACEHOLDER}");`,
        "",
        "var response = await client.PostAsJsonAsync(",
        `    "${url}",`,
        "    new",
        "    {",
        `        channelId = "${channelId}",`,
        `        ttlSeconds = ${DEFAULT_TTL_SECONDS},`,
        `        maxInvocations = ${DEFAULT_MAX_INVOCATIONS},`,
        ...(hasParameters ? [`        parameters = new Dictionary<string, object?> { ${parameterEntries} },`] : []),
        "    });",
        "response.EnsureSuccessStatusCode();",
        "",
        "var mint = await response.Content.ReadFromJsonAsync<JsonElement>();",
        'var url = mint.GetProperty("url").GetString();',
    ].join("\n");
}

function buildPythonSnippet(
    url: string,
    channelId: string,
    parameters: AgentParameterSummary[],
    hasParameters: boolean,
) {
    const parameterEntries = inlineSnippetEntries(parameters, "python");

    return [
        "import requests",
        "",
        "response = requests.post(",
        `    "${url}",`,
        `    headers={"X-Api-Key": "${API_KEY_PLACEHOLDER}"},`,
        "    json={",
        `        "channelId": "${channelId}",`,
        `        "ttlSeconds": ${DEFAULT_TTL_SECONDS},`,
        `        "maxInvocations": ${DEFAULT_MAX_INVOCATIONS},`,
        ...(hasParameters ? [`        "parameters": {${parameterEntries}},`] : []),
        "    },",
        ")",
        "response.raise_for_status()",
        'url = response.json()["url"]',
    ].join("\n");
}

function buildNodeSnippet(url: string, channelId: string, parameters: AgentParameterSummary[], hasParameters: boolean) {
    const parameterEntries = inlineSnippetEntries(parameters, "json");

    return [
        `const response = await fetch("${url}", {`,
        '    method: "POST",',
        "    headers: {",
        `        "X-Api-Key": "${API_KEY_PLACEHOLDER}",`,
        '        "Content-Type": "application/json",',
        "    },",
        "    body: JSON.stringify({",
        `        channelId: "${channelId}",`,
        `        ttlSeconds: ${DEFAULT_TTL_SECONDS},`,
        `        maxInvocations: ${DEFAULT_MAX_INVOCATIONS},`,
        ...(hasParameters ? [`        parameters: { ${parameterEntries} },`] : []),
        "    }),",
        "});",
        "if (!response.ok) {",
        "    throw new Error(`Minting the embed link failed: ${response.status}`);",
        "}",
        "const { url } = await response.json();",
    ].join("\n");
}
