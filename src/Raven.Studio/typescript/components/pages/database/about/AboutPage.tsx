import React, { useEffect } from "react";
import { useAppUrls } from "hooks/useAppUrls";
import {
    initView,
    statisticsViewSelectors,
} from "components/pages/database/status/statistics/store/statisticsViewSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { IndexesDatabaseStats } from "components/pages/database/status/statistics/partials/IndexesDatabaseStats";
import { StatsHeader } from "components/pages/database/status/statistics/partials/StatsHeader";
import { NonShardedViewProps } from "components/models/common";

interface AboutPageProps {
    test?: boolean;
}

export function AboutPage(props: AboutPageProps) {
    return (
        <>
            <h1>About Page</h1>
        </>
    );
}
