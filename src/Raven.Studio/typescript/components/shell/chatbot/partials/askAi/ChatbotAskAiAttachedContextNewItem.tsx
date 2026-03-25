import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotActions, ChatbotAttachedContext, chatbotSelectors } from "../../store/chatbotSlice";
import { Icon } from "components/common/Icon";
import { ReactNode, useMemo, useState } from "react";
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
import IconName from "typings/server/icons";

export default function ChatbotAskAiAttachedContextNewItem() {
    const dispatch = useAppDispatch();
    const tab = useAppSelector(chatbotSelectors.newContextTab);
    const isNewContextOpen = useAppSelector(chatbotSelectors.isNewContextOpen);

    return (
        <Dropdown
            show={isNewContextOpen}
            onToggle={() => dispatch(chatbotActions.isNewContextOpenToggled())}
            autoClose="outside"
        >
            <Dropdown.Toggle
                as={CustomDropdownToggle}
                isCaretHidden
                id="chatbot-add-context"
                variant="link"
                className="rounded-1 border border-color-light lh-base hstack"
                style={{
                    padding: "1px 6px",
                    fontSize: "10px",
                    height: "22px",
                }}
                title="Add context"
            >
                <Icon icon="plus" className="lh-base" margin="m-0" size="xs" />
            </Dropdown.Toggle>
            <Dropdown.Menu style={{ width: 300, height: 350 }} renderOnMount popperConfig={{ strategy: "fixed" }}>
                <div className="vstack h-100">
                    {tab === null && <AllTabs />}
                    {tab === "DatabaseName" && <DatabaseNameTab />}
                    {tab === "DocumentId" && <DocumentIdTab />}
                    {tab === "CollectionName" && <CollectionNameTab />}
                    {tab === "IndexName" && <IndexNameTab />}
                </div>
            </Dropdown.Menu>
        </Dropdown>
    );
}

function AllTabs() {
    const dispatch = useAppDispatch();

    return (
        <>
            <Dropdown.Header className="hstack justify-content-between">
                <div className="small-label">Add context</div>
                <Button
                    onClick={() => dispatch(chatbotActions.isNewContextOpenToggled())}
                    variant="link"
                    size="sm"
                    className="p-0 text-emphasis ms-auto"
                >
                    <Icon icon="close" margin="m-0" size="xs" />
                </Button>
            </Dropdown.Header>
            <AllTabsItem tab="DatabaseName" label="Database Name" iconName="database" />
            <AllTabsItem tab="DocumentId" label="Document ID" iconName="document" isRequiredDatabaseName />
            <AllTabsItem tab="CollectionName" label="Collection Name" iconName="documents" isRequiredDatabaseName />
            <AllTabsItem tab="IndexName" label="Index Name" iconName="index" isRequiredDatabaseName />
        </>
    );
}

interface AllTabsItemProps {
    tab: ChatbotAttachedContext["type"];
    label: ReactNode;
    iconName: IconName;
    isRequiredDatabaseName?: boolean;
}

function AllTabsItem({ tab, label, iconName, isRequiredDatabaseName }: AllTabsItemProps) {
    const dispatch = useAppDispatch();
    const databaseNameContext = useAppSelector((state) => chatbotSelectors.attachedContextById(state, "DatabaseName"));

    const isDisabled = isRequiredDatabaseName && (!databaseNameContext || databaseNameContext?.state === "excluded");

    return (
        <ConditionalPopover
            conditions={[
                {
                    isActive: isRequiredDatabaseName && !databaseNameContext,
                    message: "First, add Database Name to the context",
                },
                {
                    isActive: isRequiredDatabaseName && databaseNameContext?.state === "excluded",
                    message: "First, include Database Name by clicking on it in the attached context",
                },
            ]}
            popoverPlacement="left"
            className="w-100"
        >
            <Dropdown.Item onClick={() => dispatch(chatbotActions.newContextTabSet(tab))} disabled={isDisabled}>
                <Icon icon={iconName} />
                {label}
            </Dropdown.Item>
        </ConditionalPopover>
    );
}

function DatabaseNameTab() {
    const dispatch = useAppDispatch();
    const allDatabases = useAppSelector(databaseSelectors.allDatabases).filter((db) => !db.isDisabled);
    const [filter, setFilter] = useState("");

    const databaseOptions = useMemo(() => {
        const sortedByNameDatabases = allDatabases
            .filter((db) => db.name.toLowerCase().includes(filter.trim().toLowerCase()))
            .sort((a, b) => a.name.localeCompare(b.name));

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
            <div className="clearable-input mb-2">
                <Form.Control
                    type="text"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Search database name"
                    className="rounded-pill ps-3 pe-4"
                />
                {filter && (
                    <div className="clear-button">
                        <Button variant="secondary" size="sm" onClick={() => setFilter("")}>
                            <Icon icon="clear" margin="m-0" />
                        </Button>
                    </div>
                )}
            </div>
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
            if (!databaseName) {
                return [];
            }

            try {
                if (!idPrefix) {
                    const lastModifiedDocs = await databasesService.getDocumentsPreview(databaseName, 0, 10, undefined);
                    return lastModifiedDocs.items.map((x) => x.getId());
                }

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
    const [filter, setFilter] = useState("");

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
            <div className="clearable-input mb-2">
                <Form.Control
                    type="text"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Search collection name"
                    className="rounded-pill ps-3 pe-4"
                />
                {filter && (
                    <div className="clear-button">
                        <Button variant="secondary" size="sm" onClick={() => setFilter("")}>
                            <Icon icon="clear" margin="m-0" />
                        </Button>
                    </div>
                )}
            </div>
            {asyncGetCollections.loading && <ListSkeleton />}
            {asyncGetCollections.result?.length === 0 && !asyncGetCollections.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No collections found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetCollections.result?.length > 0 &&
                    asyncGetCollections.result
                        .filter((collectionName) => collectionName.toLowerCase().includes(filter.trim().toLowerCase()))
                        .map((collectionName) => (
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
    const [filter, setFilter] = useState("");

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
            <div className="clearable-input mb-2">
                <Form.Control
                    type="text"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                    placeholder="Search index name"
                    className="rounded-pill ps-3 pe-4"
                />
                {filter && (
                    <div className="clear-button">
                        <Button variant="secondary" size="sm" onClick={() => setFilter("")}>
                            <Icon icon="clear" margin="m-0" />
                        </Button>
                    </div>
                )}
            </div>
            {asyncGetIndexNames.loading && <ListSkeleton />}
            {asyncGetIndexNames.result?.length === 0 && !asyncGetIndexNames.loading && (
                <EmptySet compact className="hstack justify-content-center mt-3">
                    No indexes found
                </EmptySet>
            )}
            <div className="overflow-y-auto flex-grow-1">
                {asyncGetIndexNames.result?.length > 0 &&
                    asyncGetIndexNames.result
                        .filter((indexName) => indexName.toLowerCase().includes(filter.trim().toLowerCase()))
                        .map((indexName) => (
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
