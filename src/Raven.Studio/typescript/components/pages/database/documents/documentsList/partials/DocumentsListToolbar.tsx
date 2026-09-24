import queryUtil from "common/queryUtil";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { AccessPopover } from "components/common/AccessPopover";
import { Switch } from "components/common/Checkbox";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import ExportDocumentsDropdown from "components/pages/database/documents/documentsList/partials/ExportDocumentsDropdown";
import { useAppSelector } from "components/store";
import queryCriteria from "models/database/query/queryCriteria";
import { MouseEvent } from "react";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import router from "plugins/router";

interface DocumentsListToolbarProps {
    // null means all documents
    collectionName: string | null;
    isPaginated: boolean;
    // the columns come from the layout saved by the user instead of the defaults
    isCustomLayout: boolean;
    onPaginationToggle: () => void;
    onOpenColumnSettings: () => void;
    getVisibleColumnFields: () => string[];
    isDataChanged: boolean;
    onDataChangedRefresh: () => void;
}

export default function DocumentsListToolbar({
    collectionName,
    isPaginated,
    isCustomLayout,
    onPaginationToggle,
    onOpenColumnSettings,
    getVisibleColumnFields,
    isDataChanged,
    onDataChangedRefresh,
}: DocumentsListToolbarProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { appUrl } = useAppUrls();
    const { reportEvent } = useEventsCollector();

    const navigateToNewDocument = (e: MouseEvent, collection: string | null) => {
        reportEvent("document", collection ? "new-in-collection" : "new");

        const url = appUrl.forNewDoc(databaseName, collection);
        if (e.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    };

    const navigateToQuery = () => {
        const collectionNameForQuery = collectionName ?? "@all_docs";

        const query = queryCriteria.empty();
        query.queryText("from " + queryUtil.wrapWithSingleQuotes(collectionNameForQuery));
        query.name(`Recent query (${collectionNameForQuery})`);
        query.recentQuery(true);

        savedQueriesStorage.saveAndNavigate(databaseName, query.toStorageDto());
    };

    const handleDataChangedRefresh = () => {
        reportEvent("documents", "refresh");
        onDataChangedRefresh();
    };

    return (
        <div className="d-flex align-items-start justify-content-between gap-3 flex-wrap mb-3">
            <AccessPopover accessRequired="DatabaseReadWrite">
                <Dropdown as={ButtonGroup}>
                    <Button
                        variant="primary"
                        onClick={(e) => navigateToNewDocument(e, null)}
                        disabled={!hasDatabaseWriteAccess}
                    >
                        <Icon icon="new-document" />
                        New document
                    </Button>
                    {collectionName && (
                        <>
                            <Dropdown.Toggle
                                variant="primary"
                                as={CustomDropdownToggle}
                                disabled={!hasDatabaseWriteAccess}
                                title="More options"
                            />
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={(e) => navigateToNewDocument(e, collectionName)}>
                                    <Icon icon="new-document" />
                                    New document in current collection
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </>
                    )}
                </Dropdown>
            </AccessPopover>
            <div className="d-flex align-items-center gap-2 flex-wrap">
                {isDataChanged && (
                    <RichAlert
                        variant="warning"
                        className="py-0 px-1 mb-0 align-items-center"
                        childrenClassName="w-auto"
                        data-testid="data-changed-alert"
                    >
                        The data has changed. Your results may contain duplicates or non-current entries.{" "}
                        <Button variant="link" className="p-0 align-baseline" onClick={handleDataChangedRefresh}>
                            Refresh
                        </Button>
                    </RichAlert>
                )}
                <Button variant="secondary" onClick={navigateToQuery} title="Query current collection">
                    <Icon icon="query" />
                    Query
                </Button>
                {collectionName && (
                    <ExportDocumentsDropdown
                        collectionName={collectionName}
                        getVisibleColumnFields={getVisibleColumnFields}
                    />
                )}
                <Dropdown>
                    <Dropdown.Toggle
                        as={CustomDropdownToggle}
                        variant="secondary"
                        active={isCustomLayout}
                        title={isCustomLayout ? "Using custom columns and their order" : "Display settings"}
                    >
                        <Icon icon="table" />
                        Display
                    </Dropdown.Toggle>
                    <Dropdown.Menu align="end">
                        <Dropdown.Item onClick={onOpenColumnSettings}>
                            <Icon icon="table" />
                            Column layout settings
                        </Dropdown.Item>
                        <Dropdown.Divider />
                        <Dropdown.ItemText>
                            <Switch
                                selected={isPaginated}
                                toggleSelection={onPaginationToggle}
                                color="primary"
                                title="Show the documents page by page instead of scrolling"
                            >
                                Pagination
                            </Switch>
                        </Dropdown.ItemText>
                    </Dropdown.Menu>
                </Dropdown>
            </div>
        </div>
    );
}
