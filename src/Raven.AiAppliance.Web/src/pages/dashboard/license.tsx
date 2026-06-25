import { useQuery } from "@tanstack/react-query";
import { Check, CircleCheck, RefreshCw } from "lucide-react";
import { api } from "@/api/api";
import type { LicensePlan, LicenseResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Progress } from "@/components/shadcn/ui/progress";
import { cn } from "@/lib/utils";
import { ContactSheet } from "@/pages/dashboard/contact-sheet";

// Subtle brand wash used to make the trial card and the featured plan stand out.
// Defined as a CSS class (see index.css) so it layers over the card's bg-color.
const PREMIUM_CARD_GRADIENT = "card-premium-gradient";

export function DashboardLicense() {
    const licenseQuery = useQuery(api.queries.settings.license());

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">License</h1>
            </div>

            <ApiState
                isLoading={licenseQuery.isPending}
                isError={licenseQuery.isError}
                errorTitle="Could not load license"
                onRetry={() => licenseQuery.refetch()}
                loadingLabel="Loading license…"
            >
                {licenseQuery.data && (
                    <div className="space-y-8">
                        <TrialCard license={licenseQuery.data} />
                        <PlansSection plans={licenseQuery.data.plans} />
                        <HealthSection
                            license={licenseQuery.data}
                            isRefreshing={licenseQuery.isFetching}
                            onRefresh={() => licenseQuery.refetch()}
                        />
                    </div>
                )}
            </ApiState>
        </div>
    );
}

function TrialCard({ license }: { license: LicenseResponse }) {
    const isExpired = license.state === "expired";
    const progress = Math.min(100, (license.daysElapsed / license.trialLengthDays) * 100);
    const expireShortLabel = license.trialEndsLabel.split(",")[0];

    return (
        <Card className={PREMIUM_CARD_GRADIENT}>
            <CardHeader>
                {isExpired ? (
                    <>
                        <CardTitle className="text-2xl">Trial expired</CardTitle>
                        <CardDescription>
                            {license.graceEndsLabel
                                ? `Grace period ends ${license.graceEndsLabel}`
                                : `Trial ended ${license.trialEndsLabel}`}
                        </CardDescription>
                    </>
                ) : (
                    <CardTitle className="flex items-baseline gap-2">
                        <span className="text-4xl font-bold">{license.daysLeft}</span>
                        <span className="flex flex-col">
                            <span className="text-base font-semibold">days remaining</span>
                            <span className="text-sm font-normal text-muted-foreground">
                                Free trial enabled · Expires: {license.trialEndsLabel}
                            </span>
                        </span>
                    </CardTitle>
                )}
                <CardAction>
                    <ContactSheet
                        trigger={
                            <Button variant="outline" size="sm">
                                Contact us
                            </Button>
                        }
                    />
                </CardAction>
            </CardHeader>
            <CardContent className="space-y-6">
                <div>
                    <Progress value={progress} className="h-2" />
                    <div className="mt-2 flex justify-between text-xs text-muted-foreground">
                        <span>Start ({license.trialStartedLabel})</span>
                        <span>Expire ({expireShortLabel})</span>
                    </div>
                </div>

                <div className="space-y-4 border-t pt-5">
                    <div>
                        <h2 className="text-base font-semibold">What's included</h2>
                        <p className="text-sm text-muted-foreground">Full Quill instance, no caps, no card on file.</p>
                    </div>
                    <div className="grid gap-x-8 gap-y-4 sm:grid-cols-2 lg:grid-cols-3">
                        {license.includes.map((item) => (
                            <IncludedItem key={item} item={item} />
                        ))}
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}

function IncludedItem({ item }: { item: string }) {
    const [title, ...rest] = item.split(" — ");
    const description = rest.join(" — ");

    return (
        <div className="flex gap-2.5">
            <CircleCheck className="mt-0.5 size-4 shrink-0 text-emerald-500" aria-hidden="true" />
            <div>
                <div className="text-sm font-medium">{title}</div>
                {description && <div className="text-sm text-muted-foreground">{description}</div>}
            </div>
        </div>
    );
}

function PlansSection({ plans }: { plans: LicensePlan[] }) {
    return (
        <section className="space-y-4">
            <div>
                <h2 className="text-lg font-semibold">Available plans</h2>
                <p className="text-sm text-muted-foreground">Talk to sales to install a license.</p>
            </div>
            <div className="grid gap-4 md:grid-cols-3">
                {plans.map((plan) => (
                    <PlanCard key={plan.slug} plan={plan} />
                ))}
            </div>
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
                    {plan.priceSuffix && <span className="text-sm text-muted-foreground">{plan.priceSuffix}</span>}
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

function HealthSection({
    license,
    isRefreshing,
    onRefresh,
}: {
    license: LicenseResponse;
    isRefreshing: boolean;
    onRefresh: () => void;
}) {
    return (
        <section className="space-y-4">
            <div className="flex items-end justify-between gap-3">
                <div>
                    <h2 className="text-lg font-semibold">License health</h2>
                    <p className="text-sm text-muted-foreground">
                        Check the status of your license, and connectivity with the API.
                    </p>
                </div>
                <Button variant="outline" size="sm" onClick={onRefresh} disabled={isRefreshing}>
                    <RefreshCw aria-hidden="true" />
                    Refresh
                </Button>
            </div>
            <Card>
                <CardContent className="grid gap-6 sm:grid-cols-3">
                    <HealthMetric label="API" healthy={license.apiHealthy} value={license.api} />
                    <HealthMetric
                        label="Connectivity"
                        healthy={license.connectivityOK}
                        value={license.connectivityOK ? "OK" : "Unavailable"}
                    />
                    <div className="space-y-1">
                        <div className="text-xs text-muted-foreground">Last refreshed</div>
                        <div className="text-sm font-medium">{license.lastRefreshedLabel}</div>
                    </div>
                </CardContent>
            </Card>
        </section>
    );
}

function HealthMetric({ label, healthy, value }: { label: string; healthy: boolean; value: string }) {
    return (
        <div className="space-y-1">
            <div className="text-xs text-muted-foreground">{label}</div>
            <div className="flex items-center gap-2 text-sm font-medium">
                <span
                    className={cn("size-2 rounded-full", healthy ? "bg-emerald-500" : "bg-red-500")}
                    aria-hidden="true"
                />
                {value}
            </div>
        </div>
    );
}
