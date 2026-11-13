import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import {
    CellWithDocumentPreview,
    DocumentPreviewSource,
} from "components/common/virtualTable/cells/CellWithDocumentPreview";
import { useAppUrls } from "components/hooks/useAppUrls";

interface CellDocumentIdProps {
    id: string;
    collection?: string;
    databaseName: string;
    hasHyperlink: boolean;
    previewSource?: DocumentPreviewSource;
}

export default function CellDocumentId({
    id,
    collection,
    databaseName,
    hasHyperlink,
    previewSource,
}: CellDocumentIdProps) {
    const { appUrl } = useAppUrls();
    const documentUrl = appUrl.forEditDoc(id, databaseName, collection);

    const idValue = hasHyperlink ? (
        <a href={documentUrl} className="cell-link">
            {id}
        </a>
    ) : (
        <CellValue value={id} />
    );

    if (previewSource) {
        return (
            <CellWithDocumentPreview id={id} previewSource={previewSource}>
                {idValue}
            </CellWithDocumentPreview>
        );
    }

    return <CellWithCopy value={id}>{idValue}</CellWithCopy>;
}
