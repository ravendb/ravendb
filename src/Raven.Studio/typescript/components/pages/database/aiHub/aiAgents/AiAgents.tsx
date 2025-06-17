import { Icon } from "components/common/Icon";
import AiAgentsInfoHub from "./AiAgentsInfoHub";
import { AboutViewHeading } from "components/common/AboutView";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

export default function AiAgents() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { appUrl } = useAppUrls();
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
                        // value={searchCriteria.name}
                        // onChange={onSearchNameChange}
                        className="filtering-input"
                    />
                    {/* {searchCriteria.name && (
                        <div className="clear-button">
                            <Button
                                variant="secondary"
                                size="sm"
                                onClick={() =>
                                    setFilterCriteria({
                                        name: "",
                                        states: searchCriteria.states,
                                    })
                                }
                            >
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )} */}
                </div>
            </div>
            <div className="mt-2">
                <EmptySet>No agents found</EmptySet>
            </div>
        </div>
    );
}
