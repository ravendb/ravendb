import useConfirm from "components/common/ConfirmDialog";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { useAsyncCallback } from "react-async-hook";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import Badge from "react-bootstrap/Badge";
import classNames from "classnames";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import AiAgentGenerateCodeViewSheet from "components/pages/database/aiHub/aiAgents/partials/AiAgentGenerateCodeViewSheet";

interface AiAgentCardProps {
    config: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
    reloadAiAgents: () => void;
}

export default function AiAgentCard({ config, reloadAiAgents }: AiAgentCardProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const { aiAgentService } = useServices();
    const confirm = useConfirm();

    const { appUrl } = useAppUrls();

    const asyncDeleteAiAgent = useAsyncCallback(() => aiAgentService.deleteAiAgent(databaseName, config.Identifier), {
        onSuccess: reloadAiAgents,
    });

    const asyncToggleAiAgent = useAsyncCallback(
        (isDisabled: boolean) => aiAgentService.saveAiAgent(databaseName, { ...config, Disabled: isDisabled }),
        { onSuccess: reloadAiAgents }
    );

    const codeViewSheet = useViewSheet();

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
        <div className="panel-bg-1 p-2 rounded-2 border border-secondary g-col-12 g-col-xl-6 g-col-xxl-4">
            <div className="hstack mb-1">
                <h4 className="m-0 text-truncate flex-grow" title="AI Agent name">
                    {config.Name}
                </h4>
                {config.Disabled && (
                    <Badge bg="warning" className="ms-2 fs-6" pill>
                        Disabled
                    </Badge>
                )}
            </div>
            <div className="d-flex">
                <div className="text-truncate text-muted fs-5" title="AI Agent identifier">
                    {config.Identifier}
                </div>
                <Button
                    onClick={() => copyToClipboard.copy(config.Identifier, "Copied agent identifier to clipboard.")}
                    size="xs"
                    title="Copy to clipboard"
                    variant="link"
                    className="p-0"
                >
                    <Icon icon="copy-to-clipboard" margin="ms-1" />
                </Button>
            </div>
            <div className="mt-2 text-truncate" title={config.SystemPrompt}>
                {config.SystemPrompt}
            </div>
            <div className="hstack justify-content-between mt-2">
                {hasDatabaseWriteAccess && (
                    <a
                        href={appUrl.forChatAiAgent(databaseName, config.Identifier)}
                        className={classNames("btn btn-primary", { disabled: config.Disabled })}
                    >
                        <Icon icon="llm" />
                        Start new chat
                    </a>
                )}
                {hasDatabaseAdminAccess && (
                    <Dropdown>
                        <Dropdown.Toggle as={CustomDropdownToggle} isCaretHidden variant="secondary">
                            <Icon icon="more" margin="m-0" />
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item
                                onClick={() => {
                                    codeViewSheet.open({
                                        component: <AiAgentGenerateCodeViewSheet agentId={config.Identifier} />,
                                    });
                                }}
                            >
                                <Icon icon="magic-wand" />
                                Generate client code
                            </Dropdown.Item>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier)}>
                                <Icon icon="edit" />
                                Edit
                            </Dropdown.Item>
                            <Dropdown.Item href={appUrl.forEditAiAgent(databaseName, config.Identifier, true)}>
                                <Icon icon="copy" />
                                Clone
                            </Dropdown.Item>
                            <Dropdown.Item
                                onClick={() => asyncToggleAiAgent.execute(!config.Disabled)}
                                disabled={asyncToggleAiAgent.loading}
                            >
                                {asyncToggleAiAgent.loading ? (
                                    <Spinner size="sm" className="me-1" />
                                ) : (
                                    <Icon icon={config.Disabled ? "play" : "stop"} />
                                )}
                                {config.Disabled ? "Enable" : "Disable"}
                            </Dropdown.Item>{" "}
                            <Dropdown.Item
                                className="text-danger"
                                onClick={handleDelete}
                                disabled={asyncDeleteAiAgent.loading}
                            >
                                {asyncDeleteAiAgent.loading ? (
                                    <Spinner size="sm" className="me-1" />
                                ) : (
                                    <Icon icon="trash" />
                                )}
                                Delete
                            </Dropdown.Item>
                        </Dropdown.Menu>
                    </Dropdown>
                )}
            </div>
        </div>
    );
}
