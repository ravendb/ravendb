import continueTest from "common/shell/continueTest";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DocumentsPageBody from "components/pages/database/documents/documentsList/partials/DocumentsPageBody";
import { useAppSelector } from "components/store";
import { useEffect } from "react";

interface DocumentsListQueryParams {
    collection?: string;
    withStop?: string;
}

export default function DocumentsPage({ queryParams }: ReactQueryParamsProps<DocumentsListQueryParams>) {
    const collectionName = queryParams.collection || null;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const collectionsDatabaseName = useAppSelector(collectionsTrackerSelectors.databaseName);

    // Shows the "continue test" button of the shell (Knockout) when opened by the test driver
    useEffect(() => {
        continueTest.default.init({ database: databaseName, ...queryParams });
    }, [queryParams, databaseName]);

    if (!databaseName || collectionsDatabaseName !== databaseName) {
        return null;
    }

    return (
        <div className="content-padding vstack h-100">
            <DocumentsPageBody key={`${databaseName}/${collectionName}`} collectionName={collectionName} />
        </div>
    );
}
