import rqlLanguageService from "common/rqlLanguageService";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { useMemo } from "react";
import { useAsync } from "react-async-hook";
import { useServices } from "./useServices";

export default function useRqlLanguageService(): rqlLanguageService {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const { databasesService } = useServices();

    const asyncGetIndexNames = useAsync(async () => {
        const dto = await databasesService.getEssentialStats(db.name);
        return dto?.Indexes?.map((x) => x.Name);
    }, []);

    const languageService = useMemo(
        () => new rqlLanguageService(db, () => asyncGetIndexNames.result ?? [], "Select"),
        [asyncGetIndexNames.result, db]
    );

    return languageService;
}
