import useConfirm from "components/common/ConfirmDialog";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { useAsyncCallback } from "react-async-hook";
import Col from "react-bootstrap/Col";
import Dropdown from "react-bootstrap/Dropdown";

interface AiAgentCardProps {
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
}

export default function AiAgentCard({ config }: AiAgentCardProps) {
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
