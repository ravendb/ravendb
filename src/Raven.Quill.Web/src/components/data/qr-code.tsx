import { QRCodeSVG } from "qrcode.react";
import { cn } from "@/lib/utils";

const SIZE_PX = 320;
const QUIET_ZONE_MODULES = 4;

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
