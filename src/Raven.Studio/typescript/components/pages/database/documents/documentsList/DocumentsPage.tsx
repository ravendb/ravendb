import continueTest from "common/shell/continueTest";
import { LazyLoad } from "components/common/LazyLoad";
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

    const isLoading = !databaseName || collectionsDatabaseName !== databaseName;

    return (
        <div className="content-padding vstack h-100">
            {isLoading ? (
                <DocumentsPageSkeleton />
            ) : (
                <DocumentsPageBody key={`${databaseName}/${collectionName}`} collectionName={collectionName} />
            )}
        </div>
    );
}

function DocumentsPageSkeleton() {
    return (
        <>
            <LazyLoad active className="hstack justify-content-between gap-3 mb-3">
                <div style={{ width: 150, height: 32 }} />
                <div style={{ width: 300, height: 32 }} />
            </LazyLoad>
            <LazyLoad active className="vstack flex-grow-1">
                <div className="flex-grow-1" />
            </LazyLoad>
        </>
    );
}
