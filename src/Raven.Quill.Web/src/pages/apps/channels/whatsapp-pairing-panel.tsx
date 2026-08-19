import { useState } from "react";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { ApiState } from "@/components/data/api-state";
import { QrCode } from "@/components/data/qr-code";
import { StatusIndicator } from "@/components/data/status-indicator";
import { useWhatsAppPairing } from "@/pages/apps/channels/use-whatsapp-pairing";
import { WhatsAppPhoneNumberForm } from "@/pages/apps/channels/whatsapp-phone-number-form";

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
    const {
        pairing,
        isPending,
        isError,
        hasTimedOut,
        retry,
        restart,
        restartWithPhoneNumber,
        isRestarting,
        restartError,
    } = useWhatsAppPairing(slug, channelId);
    const [isEnteringPhoneNumber, setIsEnteringPhoneNumber] = useState(false);

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
                        onUsePhoneNumber={() => setIsEnteringPhoneNumber(true)}
                    />
                ) : isEnteringPhoneNumber && pairing?.state !== "Connected" ? (
                    <WhatsAppPhoneNumberForm
                        isSubmitting={isRestarting}
                        onCancel={() => setIsEnteringPhoneNumber(false)}
                        onSubmit={(phoneNumber) => {
                            setIsEnteringPhoneNumber(false);
                            restartWithPhoneNumber(phoneNumber);
                        }}
                    />
                ) : pairing?.state === "Pairing" && pairing.pairingCode ? (
                    <div className="flex flex-col gap-3">
                        <div className="w-fit rounded-lg border bg-muted/30 px-4 py-3 font-mono text-2xl tracking-[0.3em]">
                            {pairing.pairingCode}
                        </div>
                        <ol className="list-decimal space-y-1 pl-5 text-sm text-muted-foreground">
                            <li>Open WhatsApp on the phone to link.</li>
                            <li>
                                Go to <span className="font-medium text-foreground">Settings → Linked devices</span>.
                            </li>
                            <li>
                                Tap <span className="font-medium text-foreground">Link a device</span>, then{" "}
                                <span className="font-medium text-foreground">Link with phone number instead</span>.
                            </li>
                            <li>Enter the code above.</li>
                        </ol>
                        <p className="flex items-center gap-2 text-xs text-muted-foreground">
                            <Spinner className="size-3" />
                            Waiting for the code to be entered.
                        </p>
                        <Button
                            type="button"
                            variant="link"
                            className="h-auto w-fit p-0"
                            onClick={restart}
                            disabled={isRestarting}
                        >
                            Use a QR code instead
                        </Button>
                    </div>
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
                        <Button
                            type="button"
                            variant="link"
                            className="h-auto w-fit p-0"
                            onClick={() => setIsEnteringPhoneNumber(true)}
                            disabled={isRestarting}
                        >
                            Link with a phone number instead
                        </Button>
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
                        onUsePhoneNumber={() => setIsEnteringPhoneNumber(true)}
                    />
                ) : pairing?.state === "Disconnected" ? (
                    <RestartPrompt
                        message={pairing.lastError ?? "Lost connection to WhatsApp."}
                        action="Restart pairing"
                        restart={restart}
                        isRestarting={isRestarting}
                        onUsePhoneNumber={() => setIsEnteringPhoneNumber(true)}
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
    onUsePhoneNumber,
}: {
    message: string;
    action: string;
    restart: () => void;
    isRestarting: boolean;
    onUsePhoneNumber: () => void;
}) {
    return (
        <div className="flex flex-col gap-3">
            <Alert>{message}</Alert>
            <div className="flex items-center gap-3">
                <Button type="button" variant="outline" className="w-fit" onClick={restart} disabled={isRestarting}>
                    {isRestarting && <Spinner />}
                    {action}
                </Button>
                <Button
                    type="button"
                    variant="link"
                    className="h-auto p-0"
                    onClick={onUsePhoneNumber}
                    disabled={isRestarting}
                >
                    Link with a phone number instead
                </Button>
            </div>
        </div>
    );
}
