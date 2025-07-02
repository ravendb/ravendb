import { Icon } from "components/common/Icon";
import AiAgentsInfoHub from "./AiAgentsInfoHub";
import { AboutViewHeading } from "components/common/AboutView";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import Button from "react-bootstrap/Button";
import Col from "react-bootstrap/Col";
import { useState } from "react";

export default function AiAgents() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { appUrl } = useAppUrls();

    const { aiAgentService } = useServices();

    const asyncGetAiAgents = useAsync(async () => {
        const result = await aiAgentService.getAiAgents(databaseName);
        return result;
    }, [databaseName]);

    const [nameFilter, setNameFilter] = useState("");

    return (
        <div className="content-padding">
            <div className="hstack justify-content-between align-items-start">
                <AboutViewHeading title="AI Agents" icon="ai-agents" marginBottom={4} />
                <AiAgentsInfoHub />
            </div>
            <a href={appUrl.forEditAiAgent(databaseName)} className="btn btn-primary rounded-pill">
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
            {asyncGetAiAgents.result && Object.entries(asyncGetAiAgents.result).length === 0 && (
                <div className="mt-3">
                    <EmptySet>No agents found</EmptySet>
                </div>
            )}
            {asyncGetAiAgents.result && (
                <div className="d-flex flex-wrap gap-2 mt-3">
                    {Object.entries(asyncGetAiAgents.result)
                        .filter(([name]) => name.toLowerCase().includes(nameFilter.trim().toLowerCase()))
                        .map(([name, config]) => (
                            <AiAgentCard key={name} name={name} config={config} />
                        ))}
                </div>
            )}
        </div>
    );
}

interface AiAgentCardProps {
    name: string;
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
}

function AiAgentCard(props: AiAgentCardProps) {
    return (
        <Col className="panel-bg-1 p-2 rounded-2 border border-secondary" sm={12} xl={6} xxl={4}>
            <h4 className="m-0">{props.name}</h4>
            <div className="text-muted">Last run: TODO</div>
            <div className="mt-2 text-truncate" title={props.config.SystemPrompt}>
                {props.config.SystemPrompt}
            </div>
            <div className="hstack justify-content-between mt-2">
                <Button variant="primary">
                    <Icon icon="rocket" />
                    Test agent
                </Button>
                <Button variant="secondary">
                    <Icon icon="more" margin="m-0" />
                </Button>
            </div>
        </Col>
    );
}
