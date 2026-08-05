import { QRCodeSVG } from "qrcode.react";
import { cn } from "@/lib/utils";

// Always dark-on-white, even in dark mode: inverted QR codes scan unreliably.
export function QrCode({ value, label, className }: { value: string; label: string; className?: string }) {
    return (
        <div role="img" aria-label={label} className={cn("w-fit rounded-lg border bg-white p-3", className)}>
            <QRCodeSVG value={value} size={208} marginSize={0} fgColor="#000000" bgColor="#ffffff" />
        </div>
    );
}
