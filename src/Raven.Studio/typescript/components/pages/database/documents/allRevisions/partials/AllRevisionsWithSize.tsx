import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { AllRevisionsTableProps } from "components/pages/database/documents/allRevisions/common/allRevisionsTypes";
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
}: AllRevisionsTableProps) {
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

    return <AllRevisionsTable {...tableProps} />;
}
