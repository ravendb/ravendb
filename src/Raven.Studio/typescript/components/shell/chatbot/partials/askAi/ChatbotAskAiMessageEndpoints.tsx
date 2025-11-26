import { Icon } from "components/common/Icon";
import { chatbotActions, ChatbotUserActionState } from "../../store/chatbotSlice";
import { useEffect } from "react";
import Button from "react-bootstrap/Button";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors } from "../../store/chatbotSlice";
import { RunChatbotAiAssistantResultDto } from "commands/aiAssistant/runChatbotAiAssistantCommand";
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
    endpoints: RunChatbotAiAssistantResultDto["Endpoints"];
    userActionState: ChatbotUserActionState;
}

interface EndpointResult {
    toolId: string;
    endpoint: string;
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
    const alwaysAllowedEndpoints = useAppSelector(chatbotSelectors.alwaysAllowedEndpoints);

    const endpointsArray = Object.values(endpoints).flatMap((x) => x);

    const hasOnlyDeniedEndpoints = endpointsArray.every((endpoint) => deniedEndpoints.includes(endpoint));
    const hasOnlyAllowedEndpoints = endpointsArray.every((endpoint) => alwaysAllowedEndpoints.includes(endpoint));

    const asyncHandleAllow = useAsyncCallback(
        async () => {
            const actionResponses: Record<string, any> = {};

            const endpointPromises = Object.keys(endpoints).flatMap((toolId) =>
                endpoints[toolId].map(async (endpoint): Promise<EndpointResult> => {
                    const baseResult: Omit<EndpointResult, "status" | "resultText"> = {
                        toolId,
                        endpoint,
                    };

                    const response = await tryCatch(() => fetch(endpoint));
                    console.log("kalczur response", response);
                    if (response.status === "error") {
                        return { ...baseResult, status: "error", resultText: response.error };
                    }

                    if (!response.data.ok) {
                        const defaultErrorMessage = response.data.status + " " + response.data.statusText;
                        const jsonError = await tryCatch(() => response.data.json());
                        console.log("kalczur jsonError", jsonError);

                        if (jsonError.status === "success") {
                            return {
                                ...baseResult,
                                status: "error",
                                resultText: jsonError.data.Error?.slice(0, 100) ?? defaultErrorMessage,
                            };
                        }

                        return {
                            ...baseResult,
                            status: "error",
                            resultText: defaultErrorMessage,
                        };
                    }

                    const jsonResult = await tryCatch(() => response.data.json());
                    console.log("kalczur jsonResult", jsonResult);
                    if (jsonResult.status === "success") {
                        return { ...baseResult, status: "success", resultText: JSON.stringify(jsonResult.data) };
                    }

                    const textResult = await tryCatch(() => response.data.text());
                    console.log("kalczur textResult", textResult);
                    if (textResult.status === "success") {
                        return { ...baseResult, status: "success", resultText: textResult.data };
                    }

                    return { ...baseResult, status: "error", resultText: "Failed to parse the response" };
                })
            );

            const results = await Promise.all(endpointPromises);

            for (const { toolId, endpoint, status, resultText } of results) {
                if (status === "success") {
                    dispatch(
                        chatbotActions.attachedContextAdded({
                            id: `endpoint-${_.uniqueId()}`,
                            type: "Endpoints Responses",
                            label: endpoint,
                            value: resultText,
                            state: "included",
                        })
                    );
                }

                actionResponses[toolId] = {
                    [endpoint]: resultText,
                };
            }

            dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "allowed" } }));
            dispatch(chatbotActions.runChat({ actionResponses }));
        },
        {
            onError: () => {
                messagePublisher.reportError("Failed to retrieve endpoints");
            },
        }
    );

    const handleAlwaysAllow = async () => {
        dispatch(chatbotActions.alwaysAllowedEndpointsAdded(endpointsArray));
        asyncHandleAllow.execute();
    };

    const handleSkip = () => {
        const actionResponses: Record<string, any> = {};

        for (const toolId of Object.keys(endpoints)) {
            for (const endpoint of endpoints[toolId]) {
                actionResponses[toolId] = {
                    [endpoint]: "Skipped",
                };
            }
        }

        dispatch(chatbotActions.messageUpdated({ id, changes: { userActionState: "skipped" } }));
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    // Deny -> Skip all (always)
    const handleDeny = () => {
        // Skipped -> Denied
        dispatch(chatbotActions.deniedEndpointsAdded(endpointsArray));
        handleSkip();
    };

    useEffect(() => {
        if (hasOnlyAllowedEndpoints) {
            asyncHandleAllow.execute();
        }
    }, []);

    useEffect(() => {
        if (hasOnlyDeniedEndpoints) {
            handleSkip();
        }
    }, []);

    return (
        <div className="well border border-secondary rounded-2">
            <div className="fs-6 py-1 px-2 border-bottom border-secondary">
                <Icon icon="endpoint" />
                Retrieve endpoints
            </div>
            <div className="p-2">
                <ul className="vstack gap-1 ps-3">
                    {endpointsArray.map((endpoint) => (
                        <li key={endpoint} className="text-break">
                            GET {endpoint}
                        </li>
                    ))}
                </ul>
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
                        {userActionState === "skipped" && (
                            <Badge bg="secondary" className="rounded-pill">
                                <Icon icon="skip" />
                                Skipped
                            </Badge>
                        )}
                        {userActionState === "allowed" && (
                            <Badge bg="success" className="rounded-pill">
                                <Icon icon="check" />
                                Success
                            </Badge>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
