import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { InputItem } from "components/models/common";
import { SelectOptionWithCount } from "components/pages/database/documents/allRevisions/partials/AllRevisionsSelectComponents";
import { useAppSelector } from "components/store";
import { exhaustiveStringTuple } from "components/utils/common";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { useState } from "react";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export default function useAllRevisionsFilters() {
    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const [selectedType, setSelectedType] = useState<RevisionType>("All");
    const [selectedCollectionName, setSelectedCollectionName] = useState("");

    const tooltipTexts: Record<RevisionType, string> = {
        All: "Display all revisions.",
        Regular: "Display only revisions created by document creation or modification.",
        Deleted: 'Display only "Delete Revisions" created by document deletion.',
    };

    const [persistedTypeOptions, setPersistedTypeOptions] = useState<InputItem<RevisionType>[]>(
        allRevisionTypes.map((type) => ({
            value: type,
            label: type,
        }))
    );

    const asyncGetTypeOptions = useAsyncDebounce(
        async () => {
            const options: InputItem<RevisionType>[] = [];

            for (const type of allRevisionTypes) {
                const baseOption: InputItem<RevisionType> = {
                    value: type,
                    label: type,
                    popover: tooltipTexts[type],
                };

                if (!selectedCollectionName) {
                    const previewResult = await databasesService.getRevisionsPreview({
                        databaseName,
                        start: 0,
                        pageSize: 0,
                        type,
                        collection: selectedCollectionName,
                    });

                    options.push({ ...baseOption, count: previewResult.totalResultCount });
                } else {
                    options.push({ ...baseOption, count: null });
                }
            }

            return options;
        },
        [selectedCollectionName],
        500,
        {
            onSuccess: (options) => {
                setPersistedTypeOptions(options);
            },
        }
    );

    const [persistedCollectionOptions, setPersistedCollectionOptions] = useState<SelectOptionWithCount[]>(
        allCollectionNames.map((collectionName) => ({
            value: collectionName,
            label: collectionName,
            count: null,
        }))
    );

    const asyncGetCollectionOptions = useAsyncDebounce(
        async () => {
            const options: SelectOptionWithCount[] = [];

            for (const collectionName of allCollectionNames) {
                const baseOption: SelectOption = { label: collectionName, value: collectionName };

                if (selectedType === "All") {
                    const previewResult = await databasesService.getRevisionsPreview({
                        databaseName,
                        start: 0,
                        pageSize: 0,
                        type: selectedType,
                        collection: collectionName,
                    });

                    options.push({ ...baseOption, count: previewResult.totalResultCount });
                } else {
                    options.push({ ...baseOption, count: null });
                }
            }

            return options;
        },
        [selectedType],
        500,
        {
            onSuccess: (options) => {
                setPersistedCollectionOptions(options);
            },
        }
    );

    return {
        type: {
            options: persistedTypeOptions,
            isLoading: asyncGetTypeOptions.loading,
            value: selectedType,
            setValue: setSelectedType,
        },
        collection: {
            options: persistedCollectionOptions,
            isLoading: asyncGetCollectionOptions.loading,
            value: selectedCollectionName,
            setValue: setSelectedCollectionName,
        },
        reload: async () => {
            await asyncGetTypeOptions.execute();
            await asyncGetCollectionOptions.execute();
        },
    };
}

const allRevisionTypes = exhaustiveStringTuple<RevisionType>()("All", "Regular", "Deleted");
