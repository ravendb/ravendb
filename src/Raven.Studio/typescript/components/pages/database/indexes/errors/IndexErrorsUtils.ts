import moment from "moment/moment";
import genUtils from "common/generalUtils";
import { orderBy } from "lodash";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { sortBy } from "common/typeUtils";
import { NameAndCount } from "components/pages/database/indexes/errors/types";

const mapItems = (indexErrorsDto: Raven.Client.Documents.Indexes.IndexErrors[]) => {
    const mappedItems =
        indexErrorsDto.flatMap((value) => {
            return value.Errors.map(
                (errorDto: Raven.Client.Documents.Indexes.IndexingError): IndexErrorPerDocument => ({
                    ...errorDto,
                    Timestamp: moment.utc(errorDto.Timestamp).format(),
                    IndexName: value.Name,
                    LocalTime: genUtils.formatUtcDateAsLocal(errorDto.Timestamp),
                    RelativeTime: genUtils.formatDurationByDate(moment.utc(errorDto.Timestamp), true),
                })
            );
        }) ?? [];

    return orderBy(mappedItems, [(x: IndexErrorPerDocument) => x.Timestamp], ["desc"]);
};

const findNearestTimestamp = (data: Raven.Client.Documents.Indexes.IndexErrors[]) => {
    const now = new Date();
    let closestTimestamp: Date | null = null;
    let closestDifference = Infinity;

    data.forEach((entry) => {
        entry.Errors.forEach((error) => {
            const timestamp = new Date(error.Timestamp);
            const difference = Math.abs(+timestamp - +now);
            if (difference < closestDifference) {
                closestDifference = difference;
                closestTimestamp = timestamp;
            }
        });
    });

    return closestTimestamp;
};

const getErroredIndexNames = (errorInfoItems: ErrorInfoItem[]): NameAndCount[] => {
    const erroredIndexNamesResult = new Map<string, number>();

    errorInfoItems.forEach((item) => {
        item.indexErrorsCountDto.forEach((countDto) => {
            const prevCount = erroredIndexNamesResult.get(countDto.Name) || 0;
            const currentCount = countDto.Errors.reduce((count, val) => val.NumberOfErrors + count, 0);
            erroredIndexNamesResult.set(countDto.Name, prevCount + currentCount);
        });
    });

    return sortBy(
        Array.from(erroredIndexNamesResult.entries()).map(([name, count]) => ({ name, count })),
        (x) => x.name.toLocaleLowerCase()
    );
};

const getErroredActionNames = (errorInfoItems: ErrorInfoItem[]): NameAndCount[] => {
    const erroredActionNamesResult = new Map<string, number>();

    errorInfoItems.forEach((item) => {
        item.indexErrorsCountDto.forEach((countDto) => {
            countDto.Errors.forEach((error) => {
                const prevCount = erroredActionNamesResult.get(error.Action) || 0;
                const currentCount = error.NumberOfErrors;
                erroredActionNamesResult.set(error.Action, prevCount + currentCount);
            });
        });
    });

    return sortBy(
        Array.from(erroredActionNamesResult.entries()).map(([name, count]) => ({ name, count })),
        (x) => x.name.toLocaleLowerCase()
    );
};

export const indexErrorsUtils = {
    findNearestTimestamp,
    mapItems,
    getErroredIndexNames,
    getErroredActionNames,
};
