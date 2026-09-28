import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { ApiState } from "@/components/data/api-state";
import { DetailGridSkeleton } from "@/components/data/loading-skeletons";
import { CopyableCode } from "@/components/data/copyable-code";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Field, FieldError, FieldLabel } from "@/components/shadcn/ui/field";
import { Input } from "@/components/shadcn/ui/input";
import { IP_CONFIGURATION_DOCS_URL } from "@/lib/help-links";
import { containerNameForHost, isIpV4 } from "@/lib/subdomain-origin";
import { Heading, Text } from "@/components/typography";
import { api } from "@/api/api";

const NEW_IP_PLACEHOLDER = "<new-ip>";

export function DashboardIpConfiguration({ hostname = window.location.hostname }: { hostname?: string }) {
    const hasResolvableDomain = hostname.includes(".") && !isIpV4(hostname);
    const bindingQuery = useQuery({
        ...api.queries.dns.ipBinding(),
        enabled: hasResolvableDomain,
    });

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 space-y-1">
                    <Heading as="h1" variant="page">
                        IP configuration
                    </Heading>
                    <Text variant="muted" className="max-w-2xl">
                        Quill&rsquo;s domains resolve through DNS records that point to the IP address of the host where
                        Quill runs. These records must be updated whenever that address changes.
                        <span className="mt-1 block">
                            <a
                                href={IP_CONFIGURATION_DOCS_URL}
                                target="_blank"
                                rel="noreferrer"
                                className="underline underline-offset-4 hover:text-primary-strong"
                            >
                                Learn more about IP configuration
                            </a>
                            .
                        </span>
                    </Text>
                </div>
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
                    <CardTitle asChild>
                        <h2>Current IP binding</h2>
                    </CardTitle>
                    <CardDescription>The IP address Quill&rsquo;s domain currently resolves to.</CardDescription>
                </CardHeader>
                <CardContent>
                    {hasResolvableDomain ? (
                        <ApiState
                            isLoading={bindingQuery.isPending}
                            isError={bindingQuery.isError}
                            errorTitle="Could not resolve the current IP binding"
                            onRetry={() => bindingQuery.refetch()}
                            loadingLabel="Resolving DNS…"
                            skeleton={<DetailGridSkeleton count={2} />}
                        >
                            <div className="grid gap-6 sm:grid-cols-2">
                                <div className="space-y-1">
                                    <Text as="div" variant="caption">
                                        Domain
                                    </Text>
                                    <Text as="div" variant="label" className="break-all">
                                        {hostname}
                                    </Text>
                                </div>
                                <div className="space-y-1">
                                    <Text as="div" variant="caption">
                                        IP address
                                    </Text>
                                    <Text as="div" variant="label" className="tabular-nums">
                                        {bindingQuery.data?.addresses.length
                                            ? bindingQuery.data.addresses.join(", ")
                                            : "No DNS record found"}
                                    </Text>
                                </div>
                            </div>
                        </ApiState>
                    ) : (
                        <Text variant="muted">
                            This dashboard is opened directly by IP address, so there is no domain binding to show.
                        </Text>
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
                <CardTitle asChild>
                    <h2>Change the IP</h2>
                </CardTitle>
                <CardDescription>
                    Run this command on the Docker host to point Quill&rsquo;s domains (dashboard, db, public, api) at a
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
                <Text variant="caption">
                    DNS changes can take a few minutes to propagate. Refresh the current binding above to confirm the
                    update.
                </Text>
            </CardContent>
        </Card>
    );
}
