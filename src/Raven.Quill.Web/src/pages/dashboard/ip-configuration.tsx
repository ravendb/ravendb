import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { ApiState } from "@/components/data/api-state";
import { CopyableCode } from "@/components/data/copyable-code";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Field, FieldError, FieldLabel } from "@/components/shadcn/ui/field";
import { Input } from "@/components/shadcn/ui/input";
import { containerNameForHost, isIpV4 } from "@/lib/subdomain-origin";

const NEW_IP_PLACEHOLDER = "<new-ip>";
const DNS_A_RECORD_TYPE = 1;

type DnsJsonResponse = {
    Status: number;
    Answer?: Array<{ type: number; data: string }>;
};

// The browser cannot inspect DNS directly, so the current binding is read from
// the public record through DNS-over-HTTPS.
async function resolveIpBinding(hostname: string): Promise<string[]> {
    const response = await fetch(`https://dns.google/resolve?name=${encodeURIComponent(hostname)}&type=A`);
    if (!response.ok) {
        throw new Error(`DNS lookup failed with status ${response.status}`);
    }
    const result = (await response.json()) as DnsJsonResponse;
    if (result.Status !== 0) {
        throw new Error(`DNS lookup failed with response code ${result.Status}`);
    }
    return (result.Answer ?? []).flatMap((record) => (record.type === DNS_A_RECORD_TYPE ? [record.data] : []));
}

export function DashboardIpConfiguration({ hostname = window.location.hostname }: { hostname?: string }) {
    const hasResolvableDomain = hostname.includes(".") && !isIpV4(hostname);
    const bindingQuery = useQuery({
        queryKey: ["ip-configuration", "binding", hostname],
        queryFn: () => resolveIpBinding(hostname),
        enabled: hasResolvableDomain,
    });

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">IP configuration</h1>
                <Button
                    variant="outline"
                    size="sm"
                    onClick={() => bindingQuery.refetch()}
                    disabled={!hasResolvableDomain || bindingQuery.isFetching}
                >
                    <RefreshCw aria-hidden="true" />
                    Refresh
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle>Current IP binding</CardTitle>
                    <CardDescription>The IP address the appliance domain currently resolves to.</CardDescription>
                </CardHeader>
                <CardContent>
                    {hasResolvableDomain ? (
                        <ApiState
                            isLoading={bindingQuery.isPending}
                            isError={bindingQuery.isError}
                            errorTitle="Could not resolve the current IP binding"
                            onRetry={() => bindingQuery.refetch()}
                            loadingLabel="Resolving DNS…"
                        >
                            <div className="grid gap-6 sm:grid-cols-2">
                                <div className="space-y-1">
                                    <div className="text-xs text-muted-foreground">Domain</div>
                                    <div className="text-sm font-medium break-all">{hostname}</div>
                                </div>
                                <div className="space-y-1">
                                    <div className="text-xs text-muted-foreground">IP address</div>
                                    <div className="text-sm font-medium tabular-nums">
                                        {bindingQuery.data?.length
                                            ? bindingQuery.data.join(", ")
                                            : "No DNS record found"}
                                    </div>
                                </div>
                            </div>
                        </ApiState>
                    ) : (
                        <p className="text-sm text-muted-foreground">
                            This dashboard is opened directly by IP address, so there is no domain binding to show.
                        </p>
                    )}
                </CardContent>
            </Card>

            <ChangeIpCard hostname={hostname} />
        </div>
    );
}

function ChangeIpCard({ hostname }: { hostname: string }) {
    const [newIp, setNewIp] = useState("");
    const trimmedIp = newIp.trim();
    const isInvalidIp = trimmedIp !== "" && !isIpV4(trimmedIp);

    const command = `docker exec ${containerNameForHost(hostname)} update-dns --ip ${trimmedIp && !isInvalidIp ? trimmedIp : NEW_IP_PLACEHOLDER}`;

    return (
        <Card>
            <CardHeader>
                <CardTitle>Change the IP</CardTitle>
                <CardDescription>
                    Run this command on the Docker host to point the appliance domains (dashboard, db, public, api) at a
                    new IP address.
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
                <Field className="max-w-xs" data-invalid={isInvalidIp || undefined}>
                    <FieldLabel htmlFor="ip-configuration-new-ip">New IP address</FieldLabel>
                    <Input
                        id="ip-configuration-new-ip"
                        value={newIp}
                        onChange={(event) => setNewIp(event.target.value)}
                        placeholder="e.g. 10.0.0.42"
                        aria-invalid={isInvalidIp}
                        autoComplete="off"
                        spellCheck={false}
                    />
                    {isInvalidIp && <FieldError>Enter a valid IPv4 address.</FieldError>}
                </Field>
                <CopyableCode code={command} language="sh" copyLabel="Copy update-dns command" />
                <p className="text-xs text-muted-foreground">
                    DNS changes can take a few minutes to propagate. Refresh the current binding above to confirm the
                    update.
                </p>
            </CardContent>
        </Card>
    );
}
