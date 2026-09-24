import continueTest from "common/shell/continueTest";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DocumentsPageBody from "components/pages/database/documents/documentsList/partials/DocumentsPageBody";
import { useAppSelector } from "components/store";
import { useEffect } from "react";

interface DocumentsListQueryParams {
    collection?: string;
    // set by the test driver, shows the "continue test" button in the shell
    withStop?: string;
}

export default function DocumentsPage({ queryParams }: ReactQueryParamsProps<DocumentsListQueryParams>) {
    const collectionName = queryParams?.collection || null;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    useEffect(() => {
        continueTest.default.init({ database: databaseName, ...queryParams });
    }, [queryParams, databaseName]);

    return (
        <div className="content-padding vstack h-100">
            <DocumentsPageBody key={`${databaseName}/${collectionName ?? ""}`} collectionName={collectionName} />
        </div>
    );
}
