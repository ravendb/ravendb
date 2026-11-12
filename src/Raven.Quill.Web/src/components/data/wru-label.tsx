import { InfoHint } from "@/components/data/info-hint";

export const WRU_DESCRIPTION =
    "Write Request Unit. Usage is reported every 15 minutes, so recent writes may not be included yet.";

export const WRU_UNAVAILABLE_CAPTION = "Unavailable from the license server";

// Shared label for the write usage metric, shown as "WRU" with a tooltip
// expanding the abbreviation.
export function WruLabel({ suffix }: { suffix?: string }) {
    return (
        <span className="inline-flex items-center gap-1">
            WRU{suffix}
            <InfoHint content={WRU_DESCRIPTION} />
        </span>
    );
}
