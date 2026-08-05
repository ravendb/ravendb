import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { ApiState } from "@/components/data/api-state";
import { QrCode } from "@/components/data/qr-code";
import { StatusIndicator } from "@/components/data/status-indicator";
import { useWhatsAppPairing } from "@/pages/apps/channels/use-whatsapp-pairing";

// Shared by the create sheet (right after provisioning) and the channel detail page.
export function WhatsAppPairingPanel({
    slug,
    channelId,
    isChannelEnabled,
}: {
    slug: string;
    channelId: string;
    isChannelEnabled?: boolean;
}) {
    const { pairing, isPending, isError, hasTimedOut, retry, restart, isRestarting, restartError } =
        useWhatsAppPairing(slug, channelId);

    return (
        <div className="flex flex-col gap-4">
            {isChannelEnabled === false && (
                <Alert>This channel is disabled — messages won&apos;t be answered even when a phone is paired.</Alert>
            )}

            <ApiState
                isLoading={isPending}
                isError={isError}
                errorTitle="Could not load pairing status"
                onRetry={retry}
                loadingLabel="Checking pairing status..."
            >
                {hasTimedOut ? (
                    <RestartPrompt
                        message="The QR code expired."
                        action="Generate new code"
                        restart={restart}
                        isRestarting={isRestarting}
                    />
                ) : pairing?.state === "Pairing" && pairing.qr ? (
                    <div className="flex flex-col gap-3">
                        <QrCode value={pairing.qr} label="WhatsApp pairing QR code" />
                        <ol className="list-decimal space-y-1 pl-5 text-sm text-muted-foreground">
                            <li>Open WhatsApp on the phone to link.</li>
                            <li>
                                Go to <span className="font-medium text-foreground">Settings → Linked devices</span>.
                            </li>
                            <li>
                                Tap <span className="font-medium text-foreground">Link a device</span> and scan the
                                code.
                            </li>
                        </ol>
                        <p className="flex items-center gap-2 text-xs text-muted-foreground">
                            <Spinner className="size-3" />
                            Waiting for scan — the code refreshes automatically.
                        </p>
                    </div>
                ) : pairing?.state === "Connected" ? (
                    <div className="flex flex-col gap-3">
                        <div className="flex items-center gap-3">
                            <StatusIndicator tone="positive" label="Connected" />
                            {pairing.phoneNumber && <span className="font-mono text-sm">{pairing.phoneNumber}</span>}
                        </div>
                        <div className="flex flex-col gap-1.5">
                            <Button
                                type="button"
                                variant="outline"
                                className="w-fit"
                                onClick={restart}
                                disabled={isRestarting}
                            >
                                {isRestarting && <Spinner />}
                                Re-pair phone
                            </Button>
                            <p className="text-xs text-muted-foreground">
                                Re-pairing signs out the currently linked phone.
                            </p>
                        </div>
                    </div>
                ) : pairing?.state === "LoggedOut" ? (
                    <RestartPrompt
                        message="The phone was unlinked from WhatsApp."
                        action="Pair again"
                        restart={restart}
                        isRestarting={isRestarting}
                    />
                ) : pairing?.state === "Disconnected" ? (
                    <RestartPrompt
                        message={pairing.lastError ?? "Lost connection to WhatsApp."}
                        action="Restart pairing"
                        restart={restart}
                        isRestarting={isRestarting}
                    />
                ) : (
                    <p className="flex items-center gap-2 text-sm text-muted-foreground">
                        <Spinner className="size-4" />
                        Preparing QR code...
                    </p>
                )}

                {restartError && (
                    <Alert variant="destructive">
                        {restartError instanceof Error
                            ? restartError.message.split("\n")[0]
                            : "Could not restart pairing."}
                    </Alert>
                )}
            </ApiState>
        </div>
    );
}

function RestartPrompt({
    message,
    action,
    restart,
    isRestarting,
}: {
    message: string;
    action: string;
    restart: () => void;
    isRestarting: boolean;
}) {
    return (
        <div className="flex flex-col gap-3">
            <Alert>{message}</Alert>
            <Button type="button" variant="outline" className="w-fit" onClick={restart} disabled={isRestarting}>
                {isRestarting && <Spinner />}
                {action}
            </Button>
        </div>
    );
}
