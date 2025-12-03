import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotActions, chatbotSelectors } from "../../store/chatbotSlice";
import { Icon } from "components/common/Icon";
import { useMemo, useState } from "react";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Button from "react-bootstrap/Button";
import { EmptySet } from "components/common/EmptySet";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useServices } from "components/hooks/useServices";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import Form from "react-bootstrap/Form";
import { LazyLoad } from "components/common/LazyLoad";
import { useAsync } from "react-async-hook";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { clusterSelectors } from "components/common/shell/clusterSlice";

export default function ChatbotAskAiAttachedContextNewItem() {
    const tab = useAppSelector(chatbotSelectors.newContextTab);

    return (
        <Dropdown autoClose="outside" drop="up">
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                variant="link"
                className="rounded-2 border border-secondary hstack"
                style={{
                    padding: "1px 6px",
                    fontSize: "10px",
                    height: "23px",
                }}
                title="Add context"
            >
                <Icon icon="plus" margin="m-0" size="xs" />
            </Dropdown.Toggle>
            <Dropdown.Menu style={{ width: 300, height: 350, zIndex: 1000 }} className="vstack">
                {tab === null && <AllTabs />}
                {tab === "DatabaseName" && <DatabaseNameTab />}
                {tab === "DocumentId" && <DocumentIdTab />}
                {tab === "CollectionName" && <CollectionNameTab />}
                {tab === "IndexName" && <IndexNameTab />}
            </Dropdown.Menu>
        </Dropdown>
    );
}

function AllTabs() {
    const dispatch = useAppDispatch();

    const hasDatabaseName =
        useAppSelector((state) => chatbotSelectors.attachedContextById(state, "DatabaseName")) != null;

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">Add context</span>
            </Dropdown.Header>
            <Dropdown.Item onClick={() => dispatch(chatbotActions.newContextTabSet("DatabaseName"))}>
                <Icon icon="database" />
                Database Name
            </Dropdown.Item>
            <ConditionalPopover
                conditions={{
                    isActive: !hasDatabaseName,
                    message: "Please add Database Name first.",
                }}
                popoverPlacement="left"
            >
                <Dropdown.Item
                    onClick={() => dispatch(chatbotActions.newContextTabSet("DocumentId"))}
                    disabled={!hasDatabaseName}
                >
                    <Icon icon="document" />
                    Document ID
                </Dropdown.Item>
            </ConditionalPopover>
            <ConditionalPopover
                conditions={{
                    isActive: !hasDatabaseName,
                    message: "Please add Database Name first.",
                }}
                popoverPlacement="left"
            >
                <Dropdown.Item
                    onClick={() => dispatch(chatbotActions.newContextTabSet("CollectionName"))}
                    disabled={!hasDatabaseName}
                >
                    <Icon icon="document2" />
                    Collection Name
                </Dropdown.Item>
            </ConditionalPopover>
            <ConditionalPopover
                conditions={{
                    isActive: !hasDatabaseName,
                    message: "Please add Database Name first.",
                }}
                popoverPlacement="left"
            >
                <Dropdown.Item
                    onClick={() => dispatch(chatbotActions.newContextTabSet("IndexName"))}
                    disabled={!hasDatabaseName}
                >
                    <Icon icon="index" />
                    Index Name
                </Dropdown.Item>
            </ConditionalPopover>
        </>
    );
}

function DatabaseNameTab() {
    const dispatch = useAppDispatch();
    const allDatabases = useAppSelector(databaseSelectors.allDatabases).filter((db) => !db.isDisabled);

    const databaseOptions = useMemo(() => {
        const sortedByNameDatabases = [...allDatabases].sort((a, b) => a.name.localeCompare(b.name));
        return sortedByNameDatabases.map((db) => db.name);
    }, [allDatabases]);

    const handleSelect = (dbName: string) => {
        dispatch(
            chatbotActions.attachedContextUpserted({
                id: "DatabaseName",
                type: "DatabaseName",
                label: dbName,
                value: dbName,
                state: "included",
            })
        );
        dispatch(chatbotActions.newContextTabSet(null));
    };

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatbotActions.newContextTabSet(null))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    Database Name
                </span>
            </Dropdown.Header>
            <div className="overflow-y-auto flex-grow-1">
                {databaseOptions.length > 0 ? (
                    databaseOptions.map((dbName) => (
                        <Dropdown.Item key={dbName} onClick={() => handleSelect(dbName)}>
                            {dbName}
                        </Dropdown.Item>
                    ))
                ) : (
                    <EmptySet compact className="hstack justify-content-center">
                        No databases
                    </EmptySet>
                )}
            </div>
        </>
    );
}

function DocumentIdTab() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector((state) => chatbotSelectors.attachedContextById(state, "DatabaseName"))?.value;

    const [idPrefix, setIdPrefix] = useState("");

    const { databasesService } = useServices();

    const asyncGetDocumentIds = useAsyncDebounce(
        async () => {
            if (!databaseName || !idPrefix) {
                return [];
            }

            try {
                const results = await databasesService.getDocumentsMetadataByIDPrefix(idPrefix, 10, databaseName);
                return results.map((x) => x["@metadata"]["@id"]);
            } catch {
                return [];
            }
        },
        [idPrefix, databaseName],
        300
    );

    const handleSelect = (documentId: string) => {
        dispatch(
            chatbotActions.attachedContextUpserted({
                id: "DocumentId",
                type: "DocumentId",
                label: documentId,
                value: documentId,
                state: "included",
            })
        );
        dispatch(chatbotActions.newContextTabSet(null));
    };

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatbotActions.newContextTabSet(null))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    Document ID
                </span>
            </Dropdown.Header>

            <div className="clearable-input mb-2">
                <Form.Control
                    type="text"
                    value={idPrefix}
                    onChange={(e) => setIdPrefix(e.target.value)}
                    placeholder="Enter document ID prefix"
                    className="rounded-pill ps-3 pe-4"
                />
                {idPrefix && (
                    <div className="clear-button">
                        <Button variant="secondary" size="sm" onClick={() => setIdPrefix("")}>
                            <Icon icon="clear" margin="m-0" />
                        </Button>
                    </div>
                )}
            </div>
            {asyncGetDocumentIds.loading && <ListSkeleton />}
            {asyncGetDocumentIds.result?.length === 0 && !asyncGetDocumentIds.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No documents found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetDocumentIds.result?.length > 0 &&
                    asyncGetDocumentIds.result.map((documentId) => (
                        <Dropdown.Item
                            key={documentId}
                            onClick={() => handleSelect(documentId)}
                            className="text-truncate"
                            title={documentId}
                        >
                            {documentId}
                        </Dropdown.Item>
                    ))}
            </div>
        </>
    );
}

function CollectionNameTab() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector((state) => chatbotSelectors.attachedContextById(state, "DatabaseName"))?.value;

    const { tasksService } = useServices();

    const asyncGetCollections = useAsync(async () => {
        const result = await tasksService.fetchCollectionsStats(databaseName);
        return result.collections.map((x) => x.name).sort();
    }, [databaseName]);

    const handleSelect = (collectionName: string) => {
        dispatch(
            chatbotActions.attachedContextUpserted({
                id: "CollectionName",
                type: "CollectionName",
                label: collectionName,
                value: collectionName,
                state: "included",
            })
        );
        dispatch(chatbotActions.newContextTabSet(null));
    };

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatbotActions.newContextTabSet(null))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    Collection Name
                </span>
            </Dropdown.Header>
            {asyncGetCollections.loading && <ListSkeleton />}
            {asyncGetCollections.result?.length === 0 && !asyncGetCollections.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No indexes found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetCollections.result?.length > 0 &&
                    asyncGetCollections.result.map((collectionName) => (
                        <Dropdown.Item
                            key={collectionName}
                            onClick={() => handleSelect(collectionName)}
                            className="text-truncate"
                            title={collectionName}
                        >
                            {collectionName}
                        </Dropdown.Item>
                    ))}
            </div>
        </>
    );
}

function IndexNameTab() {
    const dispatch = useAppDispatch();
    const databaseName = useAppSelector((state) => chatbotSelectors.attachedContextById(state, "DatabaseName"))?.value;
    const db = useAppSelector(databaseSelectors.allDatabases).find((db) => db.name === databaseName);
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const { indexesService } = useServices();

    const location = useMemo(
        () => (db ? DatabaseUtils.getFirstLocation(db, localNodeTag) : null),
        [db.name, localNodeTag]
    );

    const asyncGetIndexNames = useAsync(async () => {
        const result = await indexesService.getStats(databaseName, location);

        return result.map((x) => x.Name).sort();
    }, [databaseName, location]);

    const handleSelect = (indexName: string) => {
        dispatch(
            chatbotActions.attachedContextUpserted({
                id: "IndexName",
                type: "IndexName",
                label: indexName,
                value: indexName,
                state: "included",
            })
        );
        dispatch(chatbotActions.newContextTabSet(null));
    };

    return (
        <>
            <Dropdown.Header>
                <span className="small-label">
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 text-emphasis"
                        onClick={() => dispatch(chatbotActions.newContextTabSet(null))}
                    >
                        <Icon icon="arrow-left" margin="me-1" />
                    </Button>
                    Index Name
                </span>
            </Dropdown.Header>
            {asyncGetIndexNames.loading && <ListSkeleton />}
            {asyncGetIndexNames.result?.length === 0 && !asyncGetIndexNames.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No indexes found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetIndexNames.result?.length > 0 &&
                    asyncGetIndexNames.result.map((indexName) => (
                        <Dropdown.Item
                            key={indexName}
                            onClick={() => handleSelect(indexName)}
                            className="text-truncate"
                            title={indexName}
                        >
                            {indexName}
                        </Dropdown.Item>
                    ))}
            </div>
        </>
    );
}

function ListSkeleton() {
    return (
        <LazyLoad active className="vstack gap-1">
            <div style={{ height: 35 }} />
            <div style={{ height: 35 }} />
            <div style={{ height: 35 }} />
        </LazyLoad>
    );
}
