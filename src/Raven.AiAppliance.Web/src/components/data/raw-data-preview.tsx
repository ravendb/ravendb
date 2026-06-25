import { ApiState } from "@/components/data/api-state";

type RawDataPreviewProps = {
    title: string;
    query: {
        data: unknown;
        isPending: boolean;
        isError: boolean;
        refetch: () => unknown;
    };
};

// Temporary, unstyled preview of an endpoint's raw JSON response. Lets us eyeball the
// shape of a new endpoint before building the real view. Replace with a proper UI later.
export function RawDataPreview({ title, query }: RawDataPreviewProps) {
    return (
        <section className="space-y-2">
            <h2 className="font-mono text-xs font-semibold text-muted-foreground">{title}</h2>
            <ApiState
                isLoading={query.isPending}
                isError={query.isError}
                errorTitle={`Could not load ${title}`}
                onRetry={() => void query.refetch()}
            >
                <pre className="max-h-96 overflow-auto rounded-lg border bg-muted/50 p-3 text-xs">
                    {JSON.stringify(query.data, null, 2)}
                </pre>
            </ApiState>
        </section>
    );
}
