import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { parseStartDate } from "@/lib/date-period";
import { getSetupStartDate } from "@/lib/license";

// The earliest date a server-wide view can select. It never changes while the app is open,
// so the license query is cached for the whole session and shared by every date picker.
export function useSetupStartDate(): Date | undefined {
    const licenseQuery = useQuery({ ...api.queries.settings.license(), staleTime: Infinity });
    return getSetupStartDate(licenseQuery.data?.response);
}

// The earliest date a per-app view can select: the day the app was created, since none of
// its conversations, messages or tokens can predate it. Falls back to the setup's first day
// while the app is still loading, or if it reports no usable creation date.
export function useAppStartDate(slug: string): Date | undefined {
    const setupStartDate = useSetupStartDate();
    const appQuery = useQuery({ ...api.queries.apps.detail(slug), staleTime: Infinity });
    return parseStartDate(appQuery.data?.createdAt) ?? setupStartDate;
}
