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
import Row from "react-bootstrap/Row";
import { useState } from "react";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import AiAgentCard from "./partials/AiAgentCard";

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
