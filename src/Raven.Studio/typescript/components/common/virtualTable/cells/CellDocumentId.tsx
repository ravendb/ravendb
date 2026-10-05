import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import { useAppUrls } from "components/hooks/useAppUrls";

interface CellDocumentIdProps {
    id: string;
    collection?: string;
    databaseName: string;
    hasHyperlink: boolean;
}

export default function CellDocumentId({ id, collection, databaseName, hasHyperlink }: CellDocumentIdProps) {
    const { appUrl } = useAppUrls();

    return (
        <CellWithCopy value={id}>
            {hasHyperlink ? (
                <a href={appUrl.forEditDoc(id, databaseName, collection)} className="cell-link">
                    {id}
                </a>
            ) : (
                <CellValue value={id} />
            )}
        </CellWithCopy>
    );
}
