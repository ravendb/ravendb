import { Icon } from "components/common/Icon";
import AiAgentsInfoHub from "./AiAgentsInfoHub";
import { AboutViewHeading } from "components/common/AboutView";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useState } from "react";
import { CustomDropdownToggle } from "components/common/Dropdown";
import Dropdown from "react-bootstrap/Dropdown";
import useConfirm from "components/common/ConfirmDialog";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";

export default function AiAgents() {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const { appUrl } = useAppUrls();

    const { aiAgentService } = useServices();

    const asyncGetAiAgents = useAsync(async () => {
        if (!db || db.isSharded || !db.name) {
            return [];
        }

        return await aiAgentService.getAiAgents(db.name);
    }, [db.name]);

    const [nameFilter, setNameFilter] = useState("");

    if (db.isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    AI Agents are not available for <Icon icon="sharding" color="shard" margin="m-0" /> sharded
                    databases
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <div className="content-padding">
            <div className="hstack justify-content-between align-items-start">
                <AboutViewHeading title="AI Agents" icon="ai-agents" marginBottom={4} />
                <AiAgentsInfoHub />
            </div>
            <a href={appUrl.forEditAiAgent(db.name)} className="btn btn-primary rounded-pill">
                <Icon icon="plus" />
                Add new
            </a>
            <div className="d-flex flex-column flex-grow mt-4">
                <div className="small-label ms-1 mb-1">Filter by name</div>
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder="eg. CustomerServiceAgent"
                        title="Filter agents (Alt+/)"
                        value={nameFilter}
                        onChange={(e) => setNameFilter(e.target.value)}
                        className="filtering-input"
                    />
                    {nameFilter && (
                        <div className="clear-button">
                            <Button variant="secondary" size="sm" onClick={() => setNameFilter("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            {asyncGetAiAgents.loading && <LoadingView />}
            {asyncGetAiAgents.result && asyncGetAiAgents.result.length === 0 && (
                <div className="mt-3">
                    <EmptySet>No agents found</EmptySet>
                </div>
            )}
            {asyncGetAiAgents.result && (
                <Row className="mt-3">
                    {asyncGetAiAgents.result
                        .filter((config) => config.Name.toLowerCase().includes(nameFilter.trim().toLowerCase()))
                        .sort((a, b) => a.Name.localeCompare(b.Name))
                        .map((config) => (
                            <AiAgentCard key={config.Identifier} config={config} />
                        ))}
                </Row>
            )}
        </div>
    );
}

interface AiAgentCardProps {
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
}

function AiAgentCard({ config }: AiAgentCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { aiAgentService } = useServices();
    const confirm = useConfirm();

    const { appUrl } = useAppUrls();

    const asyncDeleteAiAgent = useAsyncCallback(() => aiAgentService.deleteAiAgent(databaseName, config.Identifier));

    const handleDelete = async () => {
        const isConfirmed = await confirm({
            title: (
                <>
                    You&apos;re about to delete <strong>{config.Name}</strong>
                </>
            ),
            message: (
                <div className="text-center">
                    This action will permanently delete all the data and can&apos;t be undone.
                    <br />
                    If this was the action that you wanted to do, please confirm your choice, or cancel.
                </div>
            ),
            icon: "trash",
            confirmText: "Delete agent",
            actionColor: "danger",
        });

        if (isConfirmed) {
            asyncDeleteAiAgent.execute();
        }
    };

    return (
        <Col className="p-1" sm={12} xl={6} xxl={4}>
            <div className="panel-bg-1 p-2 rounded-2 border border-secondary">
                <h4 className="m-0">{config.Name}</h4>
                <div className="mt-2 text-truncate" title={config.SystemPrompt}>
                    {config.SystemPrompt}
                </div>
                <div className="hstack justify-content-between mt-2">
                    <a href={appUrl.forChatAiAgent(databaseName, config.Identifier)} className="btn btn-primary">
                        <Icon icon="llm" />
                        Start new chat
                    </a>
                    <Dropdown>
                        <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="secondary">
                            <Icon icon="more" margin="m-0" />
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier)}>
                                <Icon icon="edit" /> Edit agent
                            </Dropdown.Item>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier, true)}>
                                <Icon icon="copy" /> Clone agent
                            </Dropdown.Item>
                            <Dropdown.Item
                                className="text-danger"
                                onClick={handleDelete}
                                disabled={asyncDeleteAiAgent.loading}
                            >
                                <Icon icon="trash" /> Delete agent
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
        </Col>
    );
}
