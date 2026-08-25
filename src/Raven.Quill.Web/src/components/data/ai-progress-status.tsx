import { useEffect, useState, type ReactNode } from "react";
import { Text } from "@/components/typography";
import { SparklesIcon } from "lucide-react";

export type AiProgressStage = {
    fromSeconds: number;
    label: string;
};

export function AiProgressStatus({
    stages,
    startedAt,
    children,
}: {
    stages: AiProgressStage[];
    startedAt?: number;
    children: ReactNode;
}) {
    const elapsedSeconds = useElapsedSeconds(startedAt);
    const stage = stages.findLast((candidate) => elapsedSeconds >= candidate.fromSeconds);

    return (
        <div className="grid gap-2">
            <div className="flex items-center gap-2">
                <SparklesIcon className="size-4 shrink-0 animate-pulse text-primary-strong" aria-hidden="true" />
                <Text variant="label" as="span" className="animate-pulse" aria-live="polite">
                    {stage?.label}...
                </Text>
                <Text variant="caption" as="span" className="font-mono tabular-nums">
                    {formatElapsed(elapsedSeconds)}
                </Text>
            </div>
            <Text variant="muted">{children}</Text>
        </div>
    );
}

function useElapsedSeconds(startedAt?: number) {
    const [mountedAt] = useState(() => Date.now());
    const [now, setNow] = useState(mountedAt);

    useEffect(() => {
        const interval = setInterval(() => setNow(Date.now()), 1_000);

        return () => clearInterval(interval);
    }, []);

    return Math.max(0, Math.round((now - (startedAt ?? mountedAt)) / 1_000));
}

function formatElapsed(totalSeconds: number): string {
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;

    return minutes > 0 ? `${minutes}m ${String(seconds).padStart(2, "0")}s` : `${seconds}s`;
}
