import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { Row as ReactTableRow } from "@tanstack/react-table";
import { useState } from "react";
import Code from "components/common/Code";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";

interface IndexErrorsSheetProps {
    errorDetails: ReactTableRow<IndexErrorPerDocument>;
    allRows: ReactTableRow<IndexErrorPerDocument>[];
    initialIndex: number;
}

export default function IndexErrorsSheet({ errorDetails, allRows, initialIndex }: IndexErrorsSheetProps) {
    const { close } = useViewSheet();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();
    const [currentIndex, setCurrentIndex] = useState(initialIndex);

    const currentRow = allRows.length > 0 ? allRows[currentIndex] : errorDetails;
    const error = currentRow.original;

    const hasPrevious = currentIndex > 0;
    const hasNext = currentIndex < allRows.length - 1;

    return (
        <ViewSheet>
            <ViewSheet.Header>
                <h3 className="mb-0">
                    <Icon icon="warning" color="warning" />
                    Indexing error details
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="m-2">
                <div className="vstack gap-3">
                    {error.IndexName && (
                        <div className="d-flex justify-content-between align-items-center pb-1 border-bottom border-secondary">
                            <div className="small-label">Index name</div>
                            <a href={appUrl.forEditIndex(error.IndexName, dbName)} className="text-truncate">
                                {error.IndexName}
                            </a>
                        </div>
                    )}
                    {error.Document && (
                        <div className="d-flex justify-content-between align-items-center pb-1 border-bottom border-secondary">
                            <div className="small-label">Document ID</div>
                            <CellDocumentValue value={error.Document} databaseName={dbName} hasHyperlinkForIds />
                        </div>
                    )}
                    {error.LocalTime && (
                        <div className="d-flex justify-content-between align-items-center pb-1 border-bottom border-secondary">
                            <div className="small-label">Date</div>
                            <div className="fw-bold text-truncate">{error.LocalTime}</div>
                        </div>
                    )}
                    {error.Action && (
                        <div className="d-flex justify-content-between align-items-center pb-1 border-bottom border-secondary">
                            <div className="small-label">Action</div>
                            <div className="fw-bold text-truncate">{error.Action}</div>
                        </div>
                    )}
                    {error.Error && <Code code={error.Error} language="csharp" />}
                </div>
            </ViewSheet.Body>
            <ViewSheet.Footer className="d-flex justify-content-between">
                <div className="d-flex gap-2">
                    <Button variant="secondary" disabled={!hasPrevious} onClick={() => setCurrentIndex((i) => i - 1)}>
                        <Icon icon="arrow-left" />
                        Previous
                    </Button>
                    <Button variant="secondary" disabled={!hasNext} onClick={() => setCurrentIndex((i) => i + 1)}>
                        Next
                        <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </div>
                <Button variant="secondary" onClick={close}>
                    <Icon icon="close" />
                    Close
                </Button>
            </ViewSheet.Footer>
        </ViewSheet>
    );
}
