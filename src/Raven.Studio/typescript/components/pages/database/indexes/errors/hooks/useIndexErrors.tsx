import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useState } from "react";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { nameAndCount } from "components/pages/database/indexes/errors/types";
import { useAsync } from "react-async-hook";
import { indexErrorsUtils } from "components/pages/database/indexes/errors/IndexErrorsUtils";

export interface ErrorInfoItem {
    location: databaseLocationSpecifier;
    indexErrorsCountDto: indexErrorsCount[];
    totalErrorCount: number;
}

export const useIndexErrors = () => {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { indexesService } = useServices();

    const [errorInfoItems, setErrorInfoItems] = useState<ErrorInfoItem[]>([]);

    const [erroredIndexNames, setErroredIndexNames] = useState<nameAndCount[]>([]);
    const [erroredActionNames, setErroredActionNames] = useState<nameAndCount[]>([]);

    const fetchErrorCount = async (model: ErrorInfoItem) => {
        try {
            const results = await indexesService.getIndexesErrorCount(db.name, model.location);
            const errorsCountDto = results.Results;

            model.indexErrorsCountDto = errorsCountDto;

            model.totalErrorCount = errorsCountDto.reduce(
                (count, item) => count + item.Errors.reduce((sum, error) => sum + error.NumberOfErrors, 0),
                0
            );

            return model;
        } catch (e) {
            return model;
        }
    };

    const asyncFetchAllErrorCount = useAsync(async () => {
        const locations = DatabaseUtils.getLocations(db);
        const models: ErrorInfoItem[] = locations.map((location) => ({
            location,
            indexErrorsCountDto: [],
            totalErrorCount: 0,
        }));
        setErrorInfoItems(models);

        const errorInfoItems = await Promise.all(models.map(fetchErrorCount));

        setErroredIndexNames(indexErrorsUtils.getErroredIndexNames(errorInfoItems));
        setErroredActionNames(indexErrorsUtils.getErroredActionNames(errorInfoItems));

        return errorInfoItems;
    }, []);

    return {
        erroredIndexNames,
        erroredActionNames,
        errorInfoItems,
        asyncFetchAllErrorCount,
    };
};
