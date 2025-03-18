import { adminLogsActions } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch } from "components/store";
import { useState, useMemo } from "react";

export default function useAdminLogsFilter() {
    const dispatch = useAppDispatch();

    const [localFilter, setLocalFilter] = useState("");

    const debouncedSetFilter = useMemo(
        () => _.debounce((value: string) => dispatch(adminLogsActions.filterSet(value)), 500),
        [dispatch]
    );

    const handleFilterChange = (value: string) => {
        setLocalFilter(value);
        debouncedSetFilter(value);
    };

    return { localFilter, handleFilterChange };
}
