import { Icon } from "components/common/Icon";
import { chatbotActions, ChatbotEndpointItem, ChatbotUserActionState } from "../../store/chatbotSlice";
import { useEffect } from "react";
import Button from "react-bootstrap/Button";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors } from "../../store/chatbotSlice";
import { tryCatch } from "components/utils/common";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Dropdown from "react-bootstrap/Dropdown";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { useAsyncCallback } from "react-async-hook";
import messagePublisher from "common/messagePublisher";
import Badge from "react-bootstrap/Badge";

interface ChatbotAskAiMessageEndpointsProps {
    id: string;
    endpoints: ChatbotEndpointItem[];
    userActionState: ChatbotUserActionState;
}

interface EndpointResult {
    toolId: string;
    url: string;
    status: "success" | "error";
    resultText: string;
}

export default function ChatbotAskAiMessageEndpoints({
    id,
    endpoints,
    userActionState,
}: ChatbotAskAiMessageEndpointsProps) {
    const dispatch = useAppDispatch();

    const deniedEndpoints = useAppSelector(chatbotSelectors.deniedEndpoints);
    const isAlwaysAllowEndpointCalls = useAppSelector(chatbotSelectors.isAlwaysAllowEndpointCalls);

    const hasOnlyDeniedEndpoints = endpoints.map((x) => x.url).every((endpoint) => deniedEndpoints.includes(endpoint));

    const asyncHandleAllow = useAsyncCallback(
        async () => {
            const actionResponses: Record<string, any> = {};

            const endpointPromises = endpoints.map(async ({ toolId, url }): Promise<EndpointResult> => {
                const baseResult: Omit<EndpointResult, "status" | "resultText"> = {
                    toolId,
                    url,
                };

                const response = await tryCatch(() => fetch(url));
                if (response.status === "error") {
                    return { ...baseResult, status: "error", resultText: response.error };
                }

                if (!response.data.ok) {
                    const defaultErrorMessage = response.data.status + " " + response.data.statusText;
                    const jsonError = await tryCatch(() => response.data.json());

                    if (jsonError.status === "success") {
                        return {
                            ...baseResult,
                            status: "error",
                            resultText: jsonError.data.Error?.slice(0, 200) ?? defaultErrorMessage,
                        };
                    }

                    return {
                        ...baseResult,
                        status: "error",
                        resultText: defaultErrorMessage,
                    };
                }

                const textResult = await tryCatch(() => response.data.text());
                if (textResult.status === "success") {
                    return { ...baseResult, status: "success", resultText: textResult.data };
                }

                return { ...baseResult, status: "error", resultText: "Failed to parse the response" };
            });

            const results = await Promise.all(endpointPromises);

            for (const { toolId, url, resultText } of results) {
                if (!actionResponses[toolId]) {
                    actionResponses[toolId] = {
                        [url]: resultText,
                    };
                } else {
                    actionResponses[toolId][url] = resultText;
                }
            }

            const getUserActionState = (): ChatbotUserActionState => {
                if (results.some((x) => x.status === "error")) {
                    return "error";
                }
                if (isAlwaysAllowEndpointCalls) {
                    return "alwaysAllowed";
                }
                return "allowed";
            };

            const getEndpointState = (status: "success" | "error"): ChatbotUserActionState => {
                if (status === "error") {
                    return "error";
                }
                if (isAlwaysAllowEndpointCalls) {
                    return "alwaysAllowed";
                }
                return "allowed";
            };

            dispatch(
                chatbotActions.messageUpdated({
                    id,
                    changes: {
                        userActionState: getUserActionState(),
                        endpoints: results.map((x) => ({
                            toolId: x.toolId,
                            url: x.url,
                            state: getEndpointState(x.status),
                        })),
                    },
                })
            );
            dispatch(chatbotActions.runChat({ actionResponses }));
        },
        {
            onError: () => {
                messagePublisher.reportError("Failed to retrieve endpoints");
            },
        }
    );

    const handleAlwaysAllow = async () => {
        dispatch(chatbotActions.isAlwaysAllowEndpointCallsSet(true));
        asyncHandleAllow.execute();
    };

    const handleSkip = () => {
        const actionResponses: Record<string, any> = {};

        for (const endpoint of endpoints) {
            if (!actionResponses[endpoint.toolId]) {
                actionResponses[endpoint.toolId] = {
                    [endpoint.url]: "Skipped",
                };
            } else {
                actionResponses[endpoint.toolId][endpoint.url] = "Skipped";
            }
        }

        dispatch(
            chatbotActions.messageUpdated({
                id,
                changes: { userActionState: "skipped", endpoints: endpoints.map((x) => ({ ...x, state: "skipped" })) },
            })
        );
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    const handleDeny = () => {
        const actionResponses: Record<string, any> = {};

        for (const endpoint of endpoints) {
            if (!actionResponses[endpoint.toolId]) {
                actionResponses[endpoint.toolId] = {
                    [endpoint.url]: "Denied",
                };
            } else {
                actionResponses[endpoint.toolId][endpoint.url] = "Denied";
            }
        }

        dispatch(
            chatbotActions.messageUpdated({
                id,
                changes: { userActionState: "denied", endpoints: endpoints.map((x) => ({ ...x, state: "denied" })) },
            })
        );
        dispatch(chatbotActions.deniedEndpointsAdded(endpoints.map((x) => x.url)));
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    useEffect(() => {
        if (isAlwaysAllowEndpointCalls) {
            asyncHandleAllow.execute();
        } else if (hasOnlyDeniedEndpoints) {
            handleDeny();
        }
    }, []);

    return (
        <div className="well border border-secondary rounded-2">
            <div className="fs-6 py-1 px-2 border-bottom border-secondary">
                <Icon icon="endpoint" />
                Retrieve endpoints
            </div>
            <div className="p-2">
                <div className="vstack gap-1">
                    {endpoints.map((endpoint) => (
                        <EndpointItem key={endpoint.url} endpoint={endpoint} />
                    ))}
                </div>
                {userActionState === "waiting" ? (
                    <div className="hstack justify-content-between mt-2">
                        <Button
                            variant="secondary"
                            className="rounded-pill"
                            size="sm"
                            onClick={handleDeny}
                            disabled={asyncHandleAllow.loading}
                        >
                            Deny
                        </Button>
                        <div className="hstack gap-1">
                            <Button
                                variant="link"
                                className="text-emphasis"
                                size="xs"
                                onClick={handleSkip}
                                disabled={asyncHandleAllow.loading}
                            >
                                Skip
                            </Button>
                            <Dropdown className="button-dropdown-pill" as={ButtonGroup}>
                                <ButtonWithSpinner
                                    variant="primary"
                                    className="button-dropdown-btn"
                                    size="sm"
                                    onClick={asyncHandleAllow.execute}
                                    isSpinning={asyncHandleAllow.loading}
                                >
                                    Allow
                                </ButtonWithSpinner>
                                <Dropdown.Toggle
                                    variant="primary"
                                    className="dropdown-toggle button-dropdown-toggle"
                                    as={CustomDropdownToggle}
                                    size="sm"
                                    disabled={asyncHandleAllow.loading}
                                />
                                <Dropdown.Menu>
                                    <Dropdown.Item onClick={handleAlwaysAllow} className="fs-5">
                                        Always allow
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        </div>
                    </div>
                ) : (
                    <div className="hstack justify-content-end mt-2">
                        {userActionState === "alwaysAllowed" && (
                            <Badge bg="success" className="rounded-pill">
                                <Icon icon="check" />
                                Always allowed
                            </Badge>
                        )}
                        {userActionState === "denied" && (
                            <Badge bg="secondary" className="rounded-pill">
                                <Icon icon="cancel" />
                                Denied
                            </Badge>
                        )}
                        {userActionState === "skipped" && (
                            <Badge bg="secondary" className="rounded-pill">
                                <Icon icon="skip" />
                                Skipped
                            </Badge>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}

interface EndpointItemProps {
    endpoint: ChatbotEndpointItem;
}

function EndpointItem({ endpoint }: EndpointItemProps) {
    return (
        <div className="hstack w-100">
            <span className="text-nowrap">
                {endpoint.state === "waiting" && <span className="me-1">-</span>}
                {endpoint.state === "allowed" ||
                    (endpoint.state === "alwaysAllowed" && <Icon icon="check" color="success" />)}
                {endpoint.state === "error" && <Icon icon="warning" color="danger" />}
                {endpoint.state === "skipped" && <Icon icon="skip" />}
                {endpoint.state === "denied" && <Icon icon="cancel" />}
                GET
            </span>
            <a href={endpoint.url} target="_blank" className="ms-1 text-emphasis text-truncate" title={endpoint.url}>
                {endpoint.url}
            </a>
        </div>
    );
}
