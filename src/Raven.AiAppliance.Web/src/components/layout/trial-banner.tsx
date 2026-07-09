import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { ArrowRight, X } from "lucide-react";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { getLicenseDaysLeft } from "@/lib/license";

// Remembers the day-count the banner was dismissed at, so dismissing hides it for
// the day but the reminder returns as the trial winds down.
const DISMISSED_STORAGE_KEY = "trial-banner-dismissed-days-left";

export function TrialBanner() {
    const licenseQuery = useQuery(api.queries.settings.license());
    const license = licenseQuery.data?.response;
    const daysLeft = license ? getLicenseDaysLeft(license) : 0;
    const isTrialActive = daysLeft > 0;

    const [dismissedDaysLeft, setDismissedDaysLeft] = useState(() => localStorage.getItem(DISMISSED_STORAGE_KEY));

    if (!isTrialActive || dismissedDaysLeft === String(daysLeft)) {
        return null;
    }

    const dismiss = () => {
        localStorage.setItem(DISMISSED_STORAGE_KEY, String(daysLeft));
        setDismissedDaysLeft(String(daysLeft));
    };

    return (
        <div className="app-shell__banner bg-surface relative flex items-center justify-center gap-1.5 border-b px-12 py-2 text-center text-sm">
            <span className="text-muted-foreground">
                You have{" "}
                <span className="font-medium text-foreground">
                    {daysLeft} {daysLeft === 1 ? "day" : "days"}
                </span>{" "}
                remaining in your trial.
            </span>
            <Link
                to="/license"
                className="inline-flex items-center gap-1 font-medium text-primary transition-colors hover:text-primary/80 dark:text-brand-400 dark:hover:text-brand-300"
            >
                Compare plans
                <ArrowRight className="size-3.5" aria-hidden="true" />
            </Link>
            <Button
                variant="ghost"
                size="icon-sm"
                className="absolute right-2 text-muted-foreground hover:text-foreground"
                onClick={dismiss}
                aria-label="Dismiss trial banner"
            >
                <X />
            </Button>
        </div>
    );
}
