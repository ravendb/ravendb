import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { CopyableCode } from "@/components/data/copyable-code";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import {
    buildMintEmbedLinkUrl,
    DEFAULT_MAX_INVOCATIONS,
    DEFAULT_TTL_SECONDS,
    MAX_INVOCATIONS,
    MAX_TTL_SECONDS,
    MIN_INVOCATIONS,
    MIN_TTL_SECONDS,
} from "@/pages/apps/channels/embed-link-utils";
import { InlineCode } from "@/components/data/inline-code";

const OPEN_STORAGE_KEY = "quill-embed-api-docs-open";
const LANGUAGE_STORAGE_KEY = "quill-embed-api-docs-language";

const LANGUAGES = [
    { value: "bash", label: "cURL" },
    { value: "powershell", label: "PowerShell" },
    { value: "csharp", label: "C#" },
    { value: "python", label: "Python" },
    { value: "node", label: "Node.js" },
] as const;

type Language = (typeof LANGUAGES)[number]["value"];

function readIsOpen() {
    return localStorage.getItem(OPEN_STORAGE_KEY) !== "false";
}

function readLanguage(): Language {
    const stored = localStorage.getItem(LANGUAGE_STORAGE_KEY);
    return LANGUAGES.some((language) => language.value === stored) ? (stored as Language) : "bash";
}

type EmbedLinkApiDocsProps = {
    slug: string;
    agentId: string;
    parameterNames: string[];
};

export function EmbedLinkApiDocs({ slug, agentId, parameterNames }: EmbedLinkApiDocsProps) {
    const hasParameters = parameterNames.length > 0;
    const requests = buildRequestSnippets(slug, agentId, parameterNames);

    const [isOpen, setIsOpen] = useState(readIsOpen);
    const [language, setLanguage] = useState<Language>(readLanguage);

    const onOpenChange = (open: boolean) => {
        localStorage.setItem(OPEN_STORAGE_KEY, String(open));
        setIsOpen(open);
    };

    const onLanguageChange = (value: string) => {
        const next = value as Language;
        localStorage.setItem(LANGUAGE_STORAGE_KEY, next);
        setLanguage(next);
    };

    const fields = [
        { name: "agentId", description: "The agent this channel is bound to (already filled in)." },
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
                      description: `Values bound into the link for this agent (${parameterNames.join(", ")}); omitting a required one returns 400.`,
                  },
              ]
            : []),
    ];

    return (
        <Collapsible open={isOpen} onOpenChange={onOpenChange} className="rounded-md border bg-card p-4">
            <h2 className="text-sm font-semibold">
                <CollapsibleTrigger className="group flex w-full items-center justify-between gap-3 rounded-sm text-left focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none">
                    Generate links via the API
                    <ChevronDown
                        className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                        aria-hidden="true"
                    />
                </CollapsibleTrigger>
            </h2>

            <CollapsibleContent className="mt-4 grid gap-4">
                <p className="text-sm text-muted-foreground">
                    Mint links from your own backend by POSTing to the embed-links endpoint, authenticated with your
                    operator key in the <InlineCode>X-Api-Key</InlineCode> header. The app and agent are already filled
                    in below — swap in your <InlineCode>QUILL_API_KEY</InlineCode>
                    {hasParameters ? " and the parameter values" : ""}.
                </p>

                <Tabs value={language} onValueChange={onLanguageChange} className="gap-3">
                    <TabsList>
                        {LANGUAGES.map(({ value, label }) => (
                            <TabsTrigger key={value} value={value}>
                                {label}
                            </TabsTrigger>
                        ))}
                    </TabsList>
                    {LANGUAGES.map(({ value }) => (
                        <TabsContent key={value} value={value}>
                            <CopyableCode code={requests[value]} copyLabel="Copy API request" />
                        </TabsContent>
                    ))}
                </Tabs>

                <dl className="grid gap-2 text-sm">
                    {fields.map((field) => (
                        <div key={field.name} className="grid gap-x-3 sm:grid-cols-[8rem_1fr]">
                            <dt className="font-mono text-xs font-medium">{field.name}</dt>
                            <dd className="text-muted-foreground">{field.description}</dd>
                        </div>
                    ))}
                </dl>

                <p className="text-xs text-muted-foreground">
                    The response returns a <InlineCode>url</InlineCode> — use it as-is in an{" "}
                    <InlineCode>&lt;iframe src&gt;</InlineCode>. It is served only on the public embed host.
                </p>
            </CollapsibleContent>
        </Collapsible>
    );
}

const API_KEY_PLACEHOLDER = "<your QUILL_API_KEY>";

function buildRequestSnippets(slug: string, agentId: string, parameterNames: string[]): Record<Language, string> {
    const url = buildMintEmbedLinkUrl(slug);
    const hasParameters = parameterNames.length > 0;

    return {
        bash: buildCurlSnippet(url, agentId, parameterNames, "curl", "\\"),
        powershell: buildCurlSnippet(url, agentId, parameterNames, "curl.exe", "`"),
        csharp: buildCSharpSnippet(url, agentId, parameterNames, hasParameters),
        python: buildPythonSnippet(url, agentId, parameterNames, hasParameters),
        node: buildNodeSnippet(url, agentId, parameterNames, hasParameters),
    };
}

// bash continues lines with "\", PowerShell with a backtick; PowerShell also needs curl.exe so
// it doesn't resolve to the Invoke-WebRequest alias on Windows PowerShell 5.1.
function buildCurlSnippet(url: string, agentId: string, parameterNames: string[], curl: string, continuation: string) {
    const body: Record<string, unknown> = {
        agentId,
        ttlSeconds: DEFAULT_TTL_SECONDS,
        maxInvocations: DEFAULT_MAX_INVOCATIONS,
    };
    if (parameterNames.length > 0) {
        body.parameters = Object.fromEntries(parameterNames.map((name) => [name, "<value>"]));
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

function buildCSharpSnippet(url: string, agentId: string, parameterNames: string[], hasParameters: boolean) {
    const parameterEntries = parameterNames.map((name) => `[${JSON.stringify(name)}] = "<value>"`).join(", ");

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
        `        agentId = "${agentId}",`,
        `        ttlSeconds = ${DEFAULT_TTL_SECONDS},`,
        `        maxInvocations = ${DEFAULT_MAX_INVOCATIONS},`,
        ...(hasParameters ? [`        parameters = new Dictionary<string, string> { ${parameterEntries} },`] : []),
        "    });",
        "response.EnsureSuccessStatusCode();",
        "",
        "var mint = await response.Content.ReadFromJsonAsync<JsonElement>();",
        'var url = mint.GetProperty("url").GetString();',
    ].join("\n");
}

function buildPythonSnippet(url: string, agentId: string, parameterNames: string[], hasParameters: boolean) {
    const parameterEntries = parameterNames.map((name) => `${JSON.stringify(name)}: "<value>"`).join(", ");

    return [
        "import requests",
        "",
        "response = requests.post(",
        `    "${url}",`,
        `    headers={"X-Api-Key": "${API_KEY_PLACEHOLDER}"},`,
        "    json={",
        `        "agentId": "${agentId}",`,
        `        "ttlSeconds": ${DEFAULT_TTL_SECONDS},`,
        `        "maxInvocations": ${DEFAULT_MAX_INVOCATIONS},`,
        ...(hasParameters ? [`        "parameters": {${parameterEntries}},`] : []),
        "    },",
        ")",
        "response.raise_for_status()",
        'url = response.json()["url"]',
    ].join("\n");
}

function buildNodeSnippet(url: string, agentId: string, parameterNames: string[], hasParameters: boolean) {
    const parameterEntries = parameterNames.map((name) => `${JSON.stringify(name)}: "<value>"`).join(", ");

    return [
        `const response = await fetch("${url}", {`,
        '    method: "POST",',
        "    headers: {",
        `        "X-Api-Key": "${API_KEY_PLACEHOLDER}",`,
        '        "Content-Type": "application/json",',
        "    },",
        "    body: JSON.stringify({",
        `        agentId: "${agentId}",`,
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
