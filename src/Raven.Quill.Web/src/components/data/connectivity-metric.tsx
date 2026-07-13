import type { ConnectivityStatus } from "@/api/generated/server-api";
import { cn } from "@/lib/utils";

// Healthy means the licensing API answered OK with no transport error.
function isConnectivityHealthy(connectivity: ConnectivityStatus) {
    return connectivity.statusCode === "OK" && !connectivity.exception;
}

export function ConnectivityMetric({ connectivity }: { connectivity: ConnectivityStatus }) {
    return (
        <div className="space-y-2">
            <div className="space-y-1">
                <div className="text-xs text-muted-foreground">Connectivity</div>
                <div className="flex items-center gap-2 text-sm font-medium">
                    <span
                        className={cn(
                            "size-2 rounded-full",
                            isConnectivityHealthy(connectivity) ? "bg-emerald-500" : "bg-red-500",
                        )}
                        aria-hidden="true"
                    />
                    {connectivity.statusCode}
                </div>
            </div>
            {connectivity.exception && (
                <p className="text-sm break-words text-muted-foreground">{connectivity.exception}</p>
            )}
        </div>
    );
}
