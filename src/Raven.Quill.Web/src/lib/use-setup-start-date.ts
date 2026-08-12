import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { getSetupStartDate } from "@/lib/license";

// The earliest date any view can select. It never changes while the app is open, so the
// license query is cached for the whole session and shared by every date picker.
export function useSetupStartDate(): Date | undefined {
    const licenseQuery = useQuery({ ...api.queries.settings.license(), staleTime: Infinity });
    return getSetupStartDate(licenseQuery.data?.response);
}
