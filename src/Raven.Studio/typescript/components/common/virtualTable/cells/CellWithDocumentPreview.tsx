import "./CellWithDocumentPreview.scss";
import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { PreviewLine, getDocumentPreviewLines } from "components/common/virtualTable/utils/documentPreviewLines";
import document from "models/database/documents/document";
import documentMetadata from "models/database/documents/documentMetadata";
import { PropsWithChildren, useState } from "react";
import Button from "react-bootstrap/Button";
import IconName from "typings/server/icons";

export interface DocumentPreview {
    document: document;
    revisionsCount: number;
}

export interface DocumentPreviewSource {
    load: () => Promise<DocumentPreview | null>;
}

type LoadedPreview = { preview: DocumentPreview | null } | { hasFailed: true };

interface CellWithDocumentPreviewProps extends PropsWithChildren {
    id: string;
    previewSource: DocumentPreviewSource;
}

interface PreviewBadge {
    flag: "attachments" | "revisions" | "counters" | "time-series";
    icon: IconName;
    count: number;
    title: string;
}

const MAX_VISIBLE_LINES = 13;
const OPEN_DELAY_IN_MS = 500;
const LOADING_LINE_WIDTHS = ["58%", "44%", "70%", "36%", "62%", "50%", "28%"];

export function CellWithDocumentPreview({ id, previewSource, children }: CellWithDocumentPreviewProps) {
    const [target, setTarget] = useState<HTMLElement>();
    const [loadedPreview, setLoadedPreview] = useState<LoadedPreview>();

    const loadPreview = async () => {
        setLoadedPreview(undefined);
        try {
            setLoadedPreview({ preview: await previewSource.load() });
        } catch {
            setLoadedPreview({ hasFailed: true });
        }
    };

    return (
        <>
            <div ref={setTarget} className="cell-preview-target document-preview-target">
                {children}
            </div>
            <PopoverWithHover
                target={target}
                placement="right"
                className="document-preview-popover"
                prepare={loadPreview}
                openDelayInMs={OPEN_DELAY_IN_MS}
            >
                <div className="document-preview-header">
                    <span className="document-preview-id" title={id}>
                        {id}
                    </span>
                    <Button
                        variant="link"
                        title="Copy ID"
                        className="document-preview-copy"
                        onClick={() => copyToClipboard.copy(id, "Document ID has been copied to clipboard")}
                    >
                        <Icon icon="copy" margin="m-0" />
                    </Button>
                </div>
                {loadedPreview ? <DocumentPreviewBody loadedPreview={loadedPreview} /> : <DocumentPreviewLoading />}
            </PopoverWithHover>
        </>
    );
}

function DocumentPreviewLoading() {
    return (
        <div className="document-preview-body">
            <LazyLoad active className="document-preview-content document-preview-loading">
                {Array.from({ length: MAX_VISIBLE_LINES }, (_, index) => (
                    <div key={index} style={{ width: LOADING_LINE_WIDTHS[index % LOADING_LINE_WIDTHS.length] }} />
                ))}
            </LazyLoad>
        </div>
    );
}

function DocumentPreviewBody({ loadedPreview }: { loadedPreview: LoadedPreview }) {
    if ("hasFailed" in loadedPreview) {
        return <div className="document-preview-status text-danger">Unable to load the document</div>;
    }

    if (!loadedPreview.preview) {
        return <div className="document-preview-status text-muted">The document no longer exists</div>;
    }

    return <DocumentPreviewContent preview={loadedPreview.preview} />;
}

function DocumentPreviewContent({ preview }: { preview: DocumentPreview }) {
    const { lines, hiddenLineCount } = getDocumentPreviewLines(toEditorDto(preview.document), MAX_VISIBLE_LINES);
    const badges = getBadges(preview);

    return (
        <div className="document-preview-body">
            <div className="document-preview-content">
                <div className="document-preview-code">
                    {lines.map((line, index) => (
                        <DocumentPreviewLine key={index} line={line} />
                    ))}
                    {hiddenLineCount > 0 && <div className="document-preview-fade" />}
                </div>
                {hiddenLineCount > 0 && (
                    <div className="document-preview-hidden-lines">
                        {`+${pluralizeHelpers.pluralize(hiddenLineCount, "line", "lines")}`}
                    </div>
                )}
            </div>
            {badges.length > 0 && (
                <div className="document-preview-badges">
                    {badges.map((badge) => (
                        <span
                            key={badge.flag}
                            title={badge.title}
                            className={classNames("document-preview-badge", badge.flag)}
                        >
                            <Icon icon={badge.icon} margin="m-0" />
                            {badge.count.toLocaleString()}
                        </span>
                    ))}
                </div>
            )}
        </div>
    );
}

function DocumentPreviewLine({ line }: { line: PreviewLine }) {
    return (
        <div className={classNames("document-preview-line", { metadata: line.isMetadata })}>
            {line.tokens.map((token, index) => (
                <span key={index} className={token.type}>
                    {token.text}
                </span>
            ))}
        </div>
    );
}

function toEditorDto(document: document): documentDto {
    const dto = document.toDto(true);
    documentMetadata.filterMetadata(dto["@metadata"]);
    return dto;
}

function getBadges({ document, revisionsCount }: DocumentPreview): PreviewBadge[] {
    const metadata = document.__metadata;
    const attachmentsCount = metadata.attachments()?.length ?? 0;
    const countersCount = metadata.counters()?.length ?? 0;
    const timeSeriesCount = metadata.timeSeries()?.length ?? 0;

    const badges: PreviewBadge[] = [
        {
            flag: "attachments",
            icon: "attachment",
            count: attachmentsCount,
            title: pluralizeHelpers.pluralize(attachmentsCount, "attachment", "attachments"),
        },
        {
            flag: "revisions",
            icon: "revisions",
            count: revisionsCount,
            title: pluralizeHelpers.pluralize(revisionsCount, "revision", "revisions"),
        },
        {
            flag: "counters",
            icon: "new-counter",
            count: countersCount,
            title: pluralizeHelpers.pluralize(countersCount, "counter", "counters"),
        },
        {
            flag: "time-series",
            icon: "new-time-series",
            count: timeSeriesCount,
            title: pluralizeHelpers.pluralize(timeSeriesCount, "time series", "time series"),
        },
    ];

    return badges.filter((badge) => badge.count > 0);
}
