import { useState } from "react";
import { Link } from "react-router";
import { Check, X } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";
import { cn } from "@/lib/utils";
import { Heading, Text } from "@/components/typography";

const DISMISSED_STORAGE_KEY_PREFIX = "quill-welcome-dismissed:";

function dismissedStorageKey(slug: string) {
    return `${DISMISSED_STORAGE_KEY_PREFIX}${slug}`;
}

function readDismissed(slug: string) {
    return localStorage.getItem(dismissedStorageKey(slug)) === "true";
}

export function WelcomePanel({ slug }: { slug: string }) {
    const appQuery = useQuery(api.queries.apps.detail(slug));
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));

    // Dismissal lives in localStorage per app. Re-read it when the route switches
    // to a different app without remounting (mirrors the sidebar pattern in app.tsx).
    const [dismissedSlug, setDismissedSlug] = useState(slug);
    const [isDismissed, setIsDismissed] = useState(() => readDismissed(slug));
    if (slug !== dismissedSlug) {
        setDismissedSlug(slug);
        setIsDismissed(readDismissed(slug));
    }

    const steps = [
        { label: "Connect a data source", isComplete: Boolean(appQuery.data?.database) },
        {
            label: "Create the first AI agent",
            isComplete: (agentsQuery.data?.length ?? 0) > 0,
            to: appRoutes.addCapability(slug, "agent"),
        },
        {
            label: "Connect a channel",
            isComplete: (channelsQuery.data?.length ?? 0) > 0,
            to: appRoutes.app(slug, "channels"),
        },
    ];

    const isConfigured = steps.every((step) => step.isComplete);

    if (isDismissed || isConfigured) {
        return null;
    }

    const dismiss = () => {
        localStorage.setItem(dismissedStorageKey(slug), "true");
        setIsDismissed(true);
    };

    return (
        <section className="rounded-lg border bg-card p-4">
            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
                <div className="space-y-1">
                    <Heading variant="subsection">Welcome to Quill</Heading>
                    <Text variant="muted">Three steps to get your first agent answering questions in production.</Text>
                </div>
                <Button size="sm" variant="ghost" className="text-muted-foreground" onClick={dismiss}>
                    Dismiss
                    <X className="size-3.5" aria-hidden="true" />
                </Button>
            </div>

            <ol className="flex flex-wrap items-center gap-x-8 gap-y-3">
                {steps.map((step, index) => (
                    <li key={step.label}>
                        <WelcomeStep position={index + 1} {...step} />
                    </li>
                ))}
            </ol>
        </section>
    );
}

type WelcomeStepProps = {
    position: number;
    label: string;
    isComplete: boolean;
    to?: string;
};

function WelcomeStep({ position, label, isComplete, to }: WelcomeStepProps) {
    if (!to) {
        return (
            <span className="flex items-center gap-2">
                <Indicator isComplete={isComplete} position={position} />
                <Text as="span" variant="label">
                    {label}
                </Text>
            </span>
        );
    }

    return (
        <Link to={to} className="group flex items-center gap-2 transition-colors hover:text-primary-strong">
            <Indicator isComplete={isComplete} position={position} />
            <Text as="span" variant="label" className="group-hover:underline">
                {label}
            </Text>
        </Link>
    );
}

function Indicator({ isComplete, position }: Pick<WelcomeStepProps, "isComplete" | "position">) {
    return (
        <span
            className={cn(
                "flex size-5 shrink-0 items-center justify-center rounded-full text-xs font-medium",
                isComplete ? "bg-emerald-500 text-white" : "border border-border bg-muted text-muted-foreground",
            )}
            aria-hidden="true"
        >
            {isComplete ? <Check className="size-3" /> : position}
        </span>
    );
}
