import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import * as yup from "yup";

interface UseDocumentColumnsProviderProps {
    value: unknown;
    databaseName: string;
    hasHyperlinkForIds: boolean;
}

export default function CellDocumentValue({
    value,
    databaseName,
    hasHyperlinkForIds,
}: UseDocumentColumnsProviderProps) {
    const { appUrl } = useAppUrls();
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const getLinkToDocument = (cellValue: unknown): string => {
        if (typeof cellValue !== "string") {
            return null;
        }

        if (cellValue.match(externalIdRegex)) {
            const extractedCollectionName = cellValue.split("/")[0].toLowerCase();
            const matchedCollection = allCollectionNames.find((collection) =>
                extractedCollectionName.startsWith(collection.toLowerCase())
            );

            return appUrl.forEditDoc(cellValue, databaseName, matchedCollection);
        }

        return null;
    };

    const documentLink = getLinkToDocument(value);
    if (hasHyperlinkForIds && documentLink) {
        return (
            <CellWithCopy value={value}>
                <a href={documentLink}>{String(value)}</a>
            </CellWithCopy>
        );
    }

    const url = getUrl(value);
    if (url) {
        return (
            <CellWithCopy value={url}>
                <a href={url}>{url}</a>
            </CellWithCopy>
        );
    }

    return (
        <CellWithCopy value={value}>
            <CellValue value={value} />
        </CellWithCopy>
    );
}

const externalIdRegex = /^\w+\/\w+/gi;

function getUrl(cellValue: unknown): string {
    try {
        return yup.string().url().validateSync(cellValue);
    } catch (_) {
        return null;
    }
}
