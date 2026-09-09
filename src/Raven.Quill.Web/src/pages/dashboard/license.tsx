import { useQuery } from "@tanstack/react-query";
import { Check, LifeBuoy, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { ConnectivityStatus, LicensePlan, ServerLicenseResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { DetailGridSkeleton } from "@/components/data/loading-skeletons";
import { ConnectivityMetric } from "@/components/data/connectivity-metric";
import { Timestamp } from "@/components/data/timestamp";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { getSupportUrl } from "@/lib/help-links";
import { cn } from "@/lib/utils";
import { Heading, Text } from "@/components/typography";

// Subtle brand wash used to make the featured plan stand out.
// Defined as a CSS class (see index.css) so it layers over the card's bg-color.
const PREMIUM_CARD_GRADIENT = "card-premium-gradient";

export function DashboardLicense() {
    const licenseQuery = useQuery(api.queries.settings.license());

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between gap-3">
                <Heading as="h1" variant="page">
                    License
                </Heading>
                <Button
                    variant="outline"
                    size="sm"
                    onClick={() => licenseQuery.refetch()}
                    disabled={licenseQuery.isFetching}
                >
                    <RefreshCw aria-hidden="true" />
                    Refresh
                </Button>
            </div>

            <ApiState
                isLoading={licenseQuery.isPending}
                isError={licenseQuery.isError}
                errorTitle="Could not load license"
                onRetry={() => licenseQuery.refetch()}
                loadingLabel="Loading license…"
                skeleton={<DetailGridSkeleton count={6} />}
            >
                {licenseQuery.data && (
                    <div className="space-y-8">
                        <LicenseSummaryCard license={licenseQuery.data.response} />
                        <PlansSection plans={licenseQuery.data.plans} />
                        <HealthSection connectivity={licenseQuery.data.connectivity} />
                    </div>
                )}
            </ApiState>
        </div>
    );
}

function LicenseSummaryCard({ license }: { license: ServerLicenseResponse }) {
    return (
        <Card>
            <CardHeader>
                <CardTitle>Current license</CardTitle>
                {license.expired && <CardDescription>This license has expired.</CardDescription>}
                <CardAction>
                    <Button variant="outline" size="sm" asChild>
                        <a href={getSupportUrl(license.id)} target="_blank" rel="noreferrer">
                            <LifeBuoy aria-hidden="true" />
                            Support
                        </a>
                    </Button>
                </CardAction>
            </CardHeader>
            <CardContent className="grid gap-6 sm:grid-cols-2">
                <div className="space-y-1">
                    <Text as="div" variant="caption">
                        Expiration date
                    </Text>
                    <Text as="div" variant="label">
                        <Timestamp value={license.expiration} dateVariant="short" textVariant="inherit" />
                    </Text>
                </div>
                <div className="space-y-1">
                    <Text as="div" variant="caption">
                        License ID
                    </Text>
                    <Text as="div" variant="label" className="tabular-nums">
                        {license.id}
                    </Text>
                </div>
            </CardContent>
        </Card>
    );
}

function PlansSection({ plans }: { plans: LicensePlan[] }) {
    return (
        <section className="space-y-4">
            <div>
                <div className="flex items-center gap-2">
                    <Heading variant="section">Available plans</Heading>
                    <Badge variant="secondary">Coming soon</Badge>
                </div>
                <Text variant="muted">Talk to sales to install a license.</Text>
            </div>
            <div aria-disabled="true" className="pointer-events-none grid gap-4 opacity-60 grayscale md:grid-cols-3">
                {plans.map((plan) => (
                    <PlanCard key={plan.slug} plan={plan} />
                ))}
            </div>
        </section>
    );
}

function HealthSection({ connectivity }: { connectivity: ConnectivityStatus }) {
    return (
        <section className="space-y-4">
            <div>
                <Heading variant="section">License health</Heading>
                <Text variant="muted">Check the status of your license, and connectivity with the API.</Text>
            </div>
            <Card>
                <CardContent>
                    <ConnectivityMetric connectivity={connectivity} />
                </CardContent>
            </Card>
        </section>
    );
}

function PlanCard({ plan }: { plan: LicensePlan }) {
    return (
        <Card className={cn(plan.featured && PREMIUM_CARD_GRADIENT)}>
            <CardHeader>
                <CardTitle>{plan.name}</CardTitle>
                <CardDescription>{plan.tagline}</CardDescription>
                {plan.featured && (
                    <CardAction>
                        <Badge>Most popular</Badge>
                    </CardAction>
                )}
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="flex items-baseline gap-1">
                    <span className="text-2xl font-bold">{plan.priceLabel}</span>
                    {plan.priceSuffix && (
                        <Text as="span" variant="muted">
                            {plan.priceSuffix}
                        </Text>
                    )}
                </div>
                <ul className="space-y-2">
                    {plan.features.map((feature) => (
                        <li key={feature} className="flex gap-2 text-sm">
                            <Check className="mt-0.5 size-4 shrink-0 text-emerald-500" aria-hidden="true" />
                            <span>{feature}</span>
                        </li>
                    ))}
                </ul>
            </CardContent>
        </Card>
    );
}
