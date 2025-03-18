import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import AllRevisionsTableNonSharded from "components/pages/database/documents/allRevisions/partials/AllRevisionsTableNonSharded";
import AllRevisionsTableSharded from "components/pages/database/documents/allRevisions/partials/AllRevisionsTableSharded";
import AllRevisionsTableSmallSample from "components/pages/database/documents/allRevisions/partials/AllRevisionsTableSmallSample";
import { useAppSelector } from "components/store";

export default function AllRevisionsWithSize({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
}: AllRevisionsTableProps) {
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;

    const tableProps = {
        width: virtualTableUtils.getTableBodyWidth(width),
        height,
        selectedType,
        selectedCollectionName,
        fetcherRef,
        selectedRows,
        setSelectedRows,
    };

    if (selectedType !== "All" && selectedCollectionName) {
        // in this case the server does not return `TotalResults` so we display part of the results
        return <AllRevisionsTableSmallSample {...tableProps} />;
    }

    if (isSharded) {
        return <AllRevisionsTableSharded {...tableProps} />;
    }

    return <AllRevisionsTableNonSharded {...tableProps} />;
}
