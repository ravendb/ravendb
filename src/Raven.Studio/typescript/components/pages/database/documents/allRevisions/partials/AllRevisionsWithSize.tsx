import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import {
    AllRevisionsPaginationProps,
    AllRevisionsTableProps,
} from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
import { allRevisionsUtils } from "components/pages/database/documents/allRevisions/common/allRevisionsUtils";
import AllRevisionsTable from "components/pages/database/documents/allRevisions/partials/AllRevisionsTable";
import AllRevisionsTableSmallSample from "components/pages/database/documents/allRevisions/partials/AllRevisionsTableSmallSample";

export default function AllRevisionsWithSize({
    width,
    height,
    selectedType,
    selectedCollectionName,
    fetcherRef,
    selectedRows,
    setSelectedRows,
    isPaginated,
    onIsPaginatedChange,
}: AllRevisionsTableProps & AllRevisionsPaginationProps) {
    const tableProps = {
        width: virtualTableUtils.getTableBodyWidth(width),
        height,
        selectedType,
        selectedCollectionName,
        fetcherRef,
        selectedRows,
        setSelectedRows,
    };

    if (allRevisionsUtils.isSmallSample(selectedType, selectedCollectionName)) {
        // in this case the server does not return `TotalResults` so we display part of the results
        return <AllRevisionsTableSmallSample {...tableProps} />;
    }

    return <AllRevisionsTable {...tableProps} isPaginated={isPaginated} onIsPaginatedChange={onIsPaginatedChange} />;
}
