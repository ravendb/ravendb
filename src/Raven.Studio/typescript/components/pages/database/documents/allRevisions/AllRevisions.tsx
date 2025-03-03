import SizeGetter from "components/common/SizeGetter";
import { Label } from "reactstrap";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import AllRevisionsWithSize from "components/pages/database/documents/allRevisions/partials/AllRevisionsWithSize";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import {
    OptionWithCount,
    SelectOptionWithCount,
    SingleValueWithCount,
} from "components/pages/database/documents/allRevisions/partials/AllRevisionsSelectComponents";
import useAllRevisionsFilters from "components/pages/database/documents/allRevisions/hooks/useAllRevisionsFilters";
import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";
import { useEffect, useRef, useState } from "react";
import useConfirm from "components/common/ConfirmDialog";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import messagePublisher from "common/messagePublisher";
import { AllRevisionsFetcherRef } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import collectionsTracker from "common/helpers/database/collectionsTracker";
import { HStack } from "components/common/utilities/HStack";
import { VStack } from "components/common/utilities/VStack";
import AllRevisionsAboutView from "components/pages/database/documents/allRevisions/partials/AllRevisionsAboutView";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { FlexGrow } from "components/common/FlexGrow";
import classNames from "classnames";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export default function AllRevisions() {
    const { type, collection, reload: reloadOptions } = useAllRevisionsFilters();
    const [selectedRows, setSelectedRows] = useState<RevisionsPreviewResultItem[]>([]);

    const fetcherRef = useRef<AllRevisionsFetcherRef>(null);

    const confirm = useConfirm();
    const { databasesService } = useServices();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    // Reset selected rows when filters change
    useEffect(() => {
        setSelectedRows([]);
    }, [type.value, collection.value]);

    const asyncRemoveRevisions = useAsyncCallback(async () => {
        const uniqueIds = Array.from(new Set(selectedRows.map((x) => x.Id)));

        for (const id of uniqueIds) {
            await databasesService.deleteRevisionsForDocuments(activeDatabaseName, {
                DocumentIds: [id],
                RevisionsChangeVectors: selectedRows.filter((x) => x.Id === id).map((x) => x.ChangeVector),
                RemoveForceCreatedRevisions: true,
            });
        }

        messagePublisher.reportSuccess(`Successfully removed ${selectedRows.length} revisions`);
        setSelectedRows([]);
        await reloadOptions();

        collectionsTracker.default
            .getAllRevisionsCollection()
            .documentCount(
                collectionsTracker.default.getAllRevisionsCollection().documentCount() - selectedRows.length
            );
    });

    const handleRemoveConfirmation = async () => {
        const isConfirmed = await confirm({
            title: (
                <span>
                    Delete selected <strong>({selectedRows.length})</strong> revisions?
                </span>
            ),
            icon: "trash",
            actionColor: "danger",
            confirmText: "Delete",
            message: (
                <RichAlert variant="warning">
                    Please be aware that this action is irreversible. <br />
                    Revisions that are removed cannot be recovered.
                </RichAlert>
            ),
        });

        if (isConfirmed) {
            await asyncRemoveRevisions.execute();
            await fetcherRef.current?.reload();
        }
    };

    return (
        <VStack className="content-padding" gap={2}>
            <VStack>
                {hasDatabaseAdminAccess && (
                    <HStack className="justify-content-between">
                        <ButtonWithSpinner
                            variant="danger"
                            onClick={handleRemoveConfirmation}
                            disabled={selectedRows.length === 0}
                            isSpinning={asyncRemoveRevisions.loading}
                            icon="trash"
                            className="w-fit-content rounded-pill"
                        >
                            Remove {selectedRows.length != 0 && selectedRows.length} revisions
                        </ButtonWithSpinner>
                        <AllRevisionsAboutView />
                    </HStack>
                )}
                <HStack gap={2} className={classNames({ "my-3": hasDatabaseAdminAccess })}>
                    <div>
                        <Label className="small-label">Filter by collection</Label>
                        <SelectCreatable
                            options={collection.options}
                            isLoading={collection.isLoading}
                            placeholder="Select collection"
                            value={collection.options.find((x) => x.value === collection.value)}
                            onChange={(x: SelectOptionWithCount<string>) => collection.setValue(x?.value ?? "")}
                            isClearable
                            components={{ Option: OptionWithCount, SingleValue: SingleValueWithCount }}
                            isRoundedPill
                        />
                    </div>
                    <div>
                        <Label className="small-label">Filter by type</Label>
                        <MultiRadioToggle<RevisionType>
                            inputItems={type.options}
                            selectedItem={type.value}
                            setSelectedItem={type.setValue}
                        />
                    </div>
                    {!hasDatabaseAdminAccess && (
                        <>
                            <FlexGrow />
                            <AllRevisionsAboutView />
                        </>
                    )}
                </HStack>
            </VStack>
            {type.value !== "All" && collection.value && (
                <RichAlert variant="warning">
                    The table displays only part of the results. When both a collection and a type other than
                    &quot;All&quot; are selected, only the first {allRevisionsUtils.smallSampleSize} results are
                    visible.
                </RichAlert>
            )}
            <SizeGetter
                isHeighRequired
                render={({ width, height }) => (
                    <AllRevisionsWithSize
                        width={width}
                        height={height}
                        selectedType={type.value}
                        selectedCollectionName={collection.value}
                        fetcherRef={fetcherRef}
                        selectedRows={selectedRows}
                        setSelectedRows={setSelectedRows}
                    />
                )}
            />
        </VStack>
    );
}
