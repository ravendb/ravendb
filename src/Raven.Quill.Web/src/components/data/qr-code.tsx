import { QRCodeSVG } from "qrcode.react";
import { cn } from "@/lib/utils";

// A WhatsApp pairing payload is ~280 chars, which needs a 61x61 module QR. Sized so
// each module stays above ~4px and with the spec's 4-module quiet zone, otherwise the
// phone decodes it only on a lucky framing and the operator has to scan twice.
const SIZE_PX = 320;
const QUIET_ZONE_MODULES = 4;

// Always dark-on-white, even in dark mode: inverted QR codes scan unreliably.
export function QrCode({ value, label, className }: { value: string; label: string; className?: string }) {
    return (
        <div role="img" aria-label={label} className={cn("w-fit rounded-lg border bg-white p-3", className)}>
            <QRCodeSVG
                value={value}
                size={SIZE_PX}
                marginSize={QUIET_ZONE_MODULES}
                fgColor="#000000"
                bgColor="#ffffff"
            />
        </div>
    );
}
