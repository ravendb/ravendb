import { RevisionsPreviewResultItem } from "commands/database/documents/getRevisionsPreviewCommand";

type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export interface AllRevisionsWithSizeProps {
    width: number;
    height: number;
}

export interface AllRevisionsFetcherRef {
    reload: () => Promise<void | pagedResultWithToken<RevisionsPreviewResultItem>>;
}

export interface AllRevisionsTableProps extends AllRevisionsWithSizeProps {
    selectedType: RevisionType;
    selectedCollectionName: string;
    fetcherRef: React.RefObject<AllRevisionsFetcherRef>;
    selectedRows: RevisionsPreviewResultItem[];
    setSelectedRows: (rows: RevisionsPreviewResultItem[]) => void;
}
