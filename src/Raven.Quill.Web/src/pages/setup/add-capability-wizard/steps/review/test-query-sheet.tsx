import { useState } from "react";
import { useParams } from "react-router";
import { useMutation } from "@tanstack/react-query";
import { AlertCircle, Check, Play, Rocket } from "lucide-react";
import { toast } from "sonner";
import { api } from "@/api/api";
import type { TestQueryResponse } from "@/api/generated/server-api";
import AceEditor from "@/components/ace-editor/ace-editor";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import { tryParseJson } from "@/lib/utils";

const QUERY_PARAMETER_PATTERN = /\$(\w+)/g;

type TestQueryButtonProps = {
    query: string;
    parametersSampleObject: string;
    onApplyQuery: (query: string) => void;
};

export function TestQueryButton({ query, parametersSampleObject, onApplyQuery }: TestQueryButtonProps) {
    const [isOpen, setIsOpen] = useState(false);
    const hasQuery = Boolean(query.trim());

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={!hasQuery}
                    title={hasQuery ? undefined : "Write a query before testing it."}
                >
                    <Rocket aria-hidden />
                    Test query
                </Button>
            </SheetTrigger>
            <SheetContent
                className="flex w-full flex-col gap-0 sm:max-w-2xl data-[side=right]:sm:max-w-2xl"
                onOpenAutoFocus={(event) => event.preventDefault()}
            >
                <SheetHeader className="border-b">
                    <SheetTitle>Test query</SheetTitle>
                    <SheetDescription>
                        Run this tool&apos;s RQL against the app database with parameter values of your choice. Edits
                        stay here until you apply them to the tool.
                    </SheetDescription>
                </SheetHeader>
                <TestQueryPanel
                    query={query}
                    parametersSampleObject={parametersSampleObject}
                    onApplyQuery={onApplyQuery}
                />
            </SheetContent>
        </Sheet>
    );
}

function TestQueryPanel({ query, parametersSampleObject, onApplyQuery }: TestQueryButtonProps) {
    const { slug = "" } = useParams();
    const [queryText, setQueryText] = useState(query);
    const parameterNames = extractParameterNames(queryText);
    const [parametersText, setParametersText] = useState(() =>
        buildInitialParameters(parameterNames, parametersSampleObject),
    );
    const [parametersError, setParametersError] = useState<string | null>(null);
    const hasQueryChanges = queryText.trim() !== query.trim();

    const runMutation = useMutation({
        mutationFn: (parameters: Record<string, unknown> | null) =>
            api.services.agents.testQuery(slug, { query: queryText, parameters }),
    });

    const applyQuery = () => {
        onApplyQuery(queryText);
        toast.success("Query applied to the tool");
    };

    const runQuery = () => {
        const parameters = parseParameters(parametersText);
        if (parameters === undefined) {
            setParametersError("Parameters must be a JSON object.");
            return;
        }
        setParametersError(null);
        runMutation.mutate(parameters);
    };

    return (
        <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
            <div className="grid gap-1">
                <Text variant="caption" as="span">
                    Query
                </Text>
                <div className="overflow-hidden rounded-md border">
                    <AceEditor
                        mode="sql"
                        value={queryText}
                        onChange={setQueryText}
                        height="100px"
                        maxHeight={240}
                        actions={[{ component: <AceEditor.FullScreenAction /> }]}
                    />
                </div>
            </div>

            {parameterNames.length > 0 && (
                <div className="grid gap-1">
                    <Text variant="caption" as="span">
                        Parameters
                    </Text>
                    <div className="overflow-hidden rounded-md border">
                        <AceEditor
                            mode="json"
                            value={parametersText}
                            onChange={setParametersText}
                            height="120px"
                            maxHeight={300}
                            validationErrorMessage={parametersError ?? undefined}
                        />
                    </div>
                    <Text variant="caption">
                        Values for {parameterNames.map((name) => `$${name}`).join(", ")}. The LLM fills these in when
                        the agent runs.
                    </Text>
                </div>
            )}

            <div className="flex items-center gap-3">
                <Button type="button" onClick={runQuery} disabled={runMutation.isPending || !queryText.trim()}>
                    {runMutation.isPending ? <Spinner /> : <Play aria-hidden />}
                    Run query
                </Button>
                {hasQueryChanges && (
                    <Button type="button" variant="outline" onClick={applyQuery} disabled={!queryText.trim()}>
                        <Check aria-hidden />
                        Apply to tool
                    </Button>
                )}
                {runMutation.data && <ResultSummary result={runMutation.data} />}
            </div>

            {runMutation.error && (
                <Alert variant="destructive">
                    <AlertCircle />
                    <AlertTitle>Query failed</AlertTitle>
                    <AlertDescription className="break-words whitespace-pre-wrap">
                        {runMutation.error.message}
                    </AlertDescription>
                </Alert>
            )}

            {runMutation.data && <QueryResults result={runMutation.data} />}
        </div>
    );
}

function ResultSummary({ result }: { result: TestQueryResponse }) {
    const shown = result.results.length;
    const scope = result.isTruncated ? `First ${shown} of ${result.totalResults}` : `${shown}`;
    const noun = shown === 1 && !result.isTruncated ? "result" : "results";

    return (
        <Text variant="caption">
            {scope} {noun} in {result.durationMs} ms
        </Text>
    );
}

function QueryResults({ result }: { result: TestQueryResponse }) {
    if (result.results.length === 0) {
        return <Text variant="caption">The query returned no results.</Text>;
    }

    return (
        <div className="grid gap-1">
            <Text variant="caption" as="span">
                Results
            </Text>
            <div className="overflow-hidden rounded-md border">
                <AceEditor
                    mode="json"
                    value={JSON.stringify(result.results, null, 4)}
                    readOnly
                    height="320px"
                    maxHeight={600}
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                />
            </div>
        </div>
    );
}

function extractParameterNames(query: string): string[] {
    return [...new Set([...query.matchAll(QUERY_PARAMETER_PATTERN)].map((match) => match[1]))];
}

function buildInitialParameters(parameterNames: string[], parametersSampleObject: string): string {
    const sample = tryParseJson<Record<string, unknown>>(parametersSampleObject);
    const hasSample = sample !== null && typeof sample === "object" && !Array.isArray(sample);
    const parameters = Object.fromEntries(
        parameterNames.map((name) => [name, hasSample ? (sample[name] ?? null) : null]),
    );

    return JSON.stringify(parameters, null, 4);
}

function parseParameters(text: string): Record<string, unknown> | null | undefined {
    if (!text.trim()) {
        return null;
    }
    const parsed = tryParseJson<unknown>(text);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        return undefined;
    }

    return parsed as Record<string, unknown>;
}
