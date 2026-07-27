import { useEffect, useState, type ReactNode } from "react";
import { SparklesIcon } from "lucide-react";

/** A label to show once the call has been running for `fromSeconds`. */
export type AiProgressStage = {
    fromSeconds: number;
    label: string;
};

/**
 * Status line for a long-running AI call: the current stage plus how long it has been going.
 * The endpoints report no progress of their own, so stages are purely time-based - they tell the
 * operator the wizard is still working and roughly how far into a typical run they are.
 */
export function AiProgressStatus({ stages, children }: { stages: AiProgressStage[]; children: ReactNode }) {
    const elapsedSeconds = useElapsedSeconds();
    const stage = stages.findLast((candidate) => elapsedSeconds >= candidate.fromSeconds);

    return (
        <div className="grid gap-2">
            <div className="flex items-center gap-2">
                <SparklesIcon className="size-4 shrink-0 animate-pulse text-primary" aria-hidden="true" />
                <span className="animate-pulse text-sm font-medium" aria-live="polite">
                    {stage?.label}...
                </span>
                <span className="font-mono text-xs text-muted-foreground tabular-nums">
                    {formatElapsed(elapsedSeconds)}
                </span>
            </div>
            <p className="text-sm text-muted-foreground">{children}</p>
        </div>
    );
}

function useElapsedSeconds() {
    const [elapsedSeconds, setElapsedSeconds] = useState(0);

    useEffect(() => {
        const interval = setInterval(() => setElapsedSeconds((seconds) => seconds + 1), 1_000);

        return () => clearInterval(interval);
    }, []);

    return elapsedSeconds;
}

function formatElapsed(totalSeconds: number): string {
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;

    return minutes > 0 ? `${minutes}m ${String(seconds).padStart(2, "0")}s` : `${seconds}s`;
}
