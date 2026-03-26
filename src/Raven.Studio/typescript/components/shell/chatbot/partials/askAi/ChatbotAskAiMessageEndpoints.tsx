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
import { chatbotConstants, ChatbotEndpointEntry } from "components/shell/chatbot/utils/chatbotConstants";
import "./ChatbotAskAiMessageEndpoints.scss";
import genUtils from "common/generalUtils";
import { ConditionalPopover } from "components/common/ConditionalPopover";

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
    resultSizeInBytes?: number;
}

export default function ChatbotAskAiMessageEndpoints({
    id,
    endpoints,
    userActionState,
}: ChatbotAskAiMessageEndpointsProps) {
    const dispatch = useAppDispatch();

    const deniedEndpoints = useAppSelector(chatbotSelectors.deniedEndpoints);
    const isAlwaysAllowEndpointCalls = useAppSelector(chatbotSelectors.isAlwaysAllowEndpointCalls);
    const isDataSubmissionEnabled = useAppSelector(chatbotSelectors.isDataSubmissionEnabled);

    const hasOnlyDeniedEndpoints = endpoints.map((x) => x.url).every((endpoint) => deniedEndpoints.includes(endpoint));

    const asyncHandleAllow = useAsyncCallback(
        async () => {
            const endpointPromises = endpoints.map(async ({ toolId, url }): Promise<EndpointResult> => {
                const baseResult: Pick<EndpointResult, "toolId" | "url"> = { toolId, url };

                if (!isOnWhitelist(url)) {
                    return { ...baseResult, status: "error", resultText: "Endpoint is not whitelisted" };
                }

                if (!isDataSubmissionEnabled && isWithDataSubmission(url)) {
                    return {
                        ...baseResult,
                        status: "error",
                        resultText: "Data submission is disabled",
                    };
                }

                const response = await tryCatch(() => fetch(url));
                if (response.status === "error") {
                    return { ...baseResult, status: "error", resultText: response.error };
                }

                if (!response.data.ok) {
                    const defaultErrorMessage = response.data.status + " " + response.data.statusText;
                    const jsonError = await tryCatch(() => response.data.json());

                    if (jsonError.status === "success") {
                        let resultText = defaultErrorMessage;

                        if (jsonError.data.Error) {
                            resultText = jsonError.data.Error.slice(0, 200);
                        } else if (jsonError.data.Message) {
                            resultText = jsonError.data.Message.slice(0, 200);
                        }

                        return {
                            ...baseResult,
                            status: "error",
                            resultText,
                        };
                    }

                    return {
                        ...baseResult,
                        status: "error",
                        resultText: defaultErrorMessage,
                    };
                }

                const exposedFieldsConfig = getExposedFieldsConfig(url);
                if (exposedFieldsConfig) {
                    const jsonResult = await tryCatch(() => response.data.json());
                    if (jsonResult.status === "error") {
                        return { ...baseResult, status: "error", resultText: "Failed to parse the response" };
                    }

                    const exposedFieldsResult = createExposedFieldsResult(jsonResult.data, exposedFieldsConfig);

                    return {
                        ...baseResult,
                        status: "success",
                        ...exposedFieldsResult,
                    };
                }

                const textResult = await tryCatch(() => response.data.text());
                if (textResult.status === "success") {
                    const resultSizeInBytes = new Blob([textResult.data]).size;
                    return { ...baseResult, status: "success", resultText: textResult.data, resultSizeInBytes };
                }

                return { ...baseResult, status: "error", resultText: "Failed to parse the response" };
            });

            const results = await Promise.all(endpointPromises);

            const actionResponses: Record<string, any> = {};

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

            let messageEndpoints: ChatbotEndpointItem[] = results.map((x) => ({
                toolId: x.toolId,
                url: x.url,
                state: getEndpointState(x.status),
                resultSizeInBytes: x.resultSizeInBytes,
            }));

            dispatch(
                chatbotActions.messageUpdated({
                    id,
                    changes: {
                        userActionState: getUserActionState(),
                        endpoints: messageEndpoints,
                    },
                })
            );

            const actionResponsesKeys = Object.keys(actionResponses);

            // Process action responses one by one to avoid RequestTooLarge error
            // If single action response is RequestTooLarge, send error message + size
            for (let i = 0; i < actionResponsesKeys.length; i++) {
                const isLastAction = i === actionResponsesKeys.length - 1;

                const [toolId, value] = Object.entries(actionResponses)[i];

                const runResult = await dispatch(
                    chatbotActions.runChat({ actionResponses: { [toolId]: value } })
                ).unwrap();

                if (!isLastAction || runResult.state === "RequestTooLarge") {
                    dispatch(chatbotActions.messageRemoved(runResult.id));
                }

                if (runResult.state === "RequestTooLarge") {
                    messageEndpoints = messageEndpoints.map((endpoint) =>
                        endpoint.toolId === toolId ? { ...endpoint, isRequestTooLarge: true } : endpoint
                    );

                    dispatch(
                        chatbotActions.messageUpdated({
                            id,
                            changes: {
                                endpoints: messageEndpoints,
                            },
                        })
                    );

                    const toolSizeInBytes = new Blob([JSON.stringify(value)]).size;
                    const retryRunResult = await dispatch(
                        chatbotActions.runChat({
                            actionResponses: {
                                [toolId]: `Error: Request too large to process (${genUtils.formatBytesToSize(toolSizeInBytes)})`,
                            },
                        })
                    ).unwrap();

                    if (!isLastAction) {
                        dispatch(chatbotActions.messageRemoved(retryRunResult.id));
                    }
                }
            }
        },
        {
            onError: (e) => {
                console.error(e);
                messagePublisher.reportError("Failed to retrieve endpoints");
            },
        }
    );

    const createActionResponsesWithSingleValue = (value: string): Record<string, any> => {
        const actionResponses: Record<string, any> = {};

        for (const endpoint of endpoints) {
            if (!actionResponses[endpoint.toolId]) {
                actionResponses[endpoint.toolId] = {
                    [endpoint.url]: value,
                };
            } else {
                actionResponses[endpoint.toolId][endpoint.url] = value;
            }
        }

        return actionResponses;
    };

    const handleAlwaysAllow = async () => {
        dispatch(chatbotActions.isAlwaysAllowEndpointCallsSet(true));
        asyncHandleAllow.execute();
    };

    const handleSkip = () => {
        dispatch(
            chatbotActions.messageUpdated({
                id,
                changes: { userActionState: "skipped", endpoints: endpoints.map((x) => ({ ...x, state: "skipped" })) },
            })
        );

        const actionResponses = createActionResponsesWithSingleValue("Skipped");
        dispatch(chatbotActions.runChat({ actionResponses }));
    };

    const handleDeny = () => {
        dispatch(
            chatbotActions.messageUpdated({
                id,
                changes: { userActionState: "denied", endpoints: endpoints.map((x) => ({ ...x, state: "denied" })) },
            })
        );
        dispatch(chatbotActions.deniedEndpointsAdded(endpoints.map((x) => x.url)));

        const actionResponses = createActionResponsesWithSingleValue("Denied");
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
        <div className="retrieve-endpoints well border border-color-light rounded-2">
            <div className="retrieve-endpoints-header border-bottom border-color-light">
                <Icon icon="endpoint" />
                Retrieve endpoints
            </div>
            <div className="p-2">
                <ul className="tree">
                    {endpoints.map((endpoint) => (
                        <EndpointItem key={endpoint.url} endpoint={endpoint} />
                    ))}
                </ul>
                {userActionState === "waiting" ? (
                    <div className="retrieve-endpoints-actions hstack justify-content-between mt-2">
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
                                    <Dropdown.Item onClick={handleAlwaysAllow}>Always allow</Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        </div>
                    </div>
                ) : (
                    <div className="hstack justify-content-end">
                        {userActionState === "alwaysAllowed" && (
                            <Badge bg="success" className="rounded-pill mt-2">
                                <Icon icon="check" />
                                Always allowed
                            </Badge>
                        )}
                        {userActionState === "denied" && (
                            <Badge bg="secondary" className="rounded-pill mt-2">
                                <Icon icon="cancel" />
                                Denied
                            </Badge>
                        )}
                        {userActionState === "skipped" && (
                            <Badge bg="secondary" className="rounded-pill mt-2">
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
    const urlObject = new URL(endpoint.url, window.location.origin);
    const urlWithParamToDisplay = chatbotConstants.paramToDisplayRegexEndpoints.find(({ regex }) =>
        regex.test(urlObject.pathname)
    );

    const paramIds = urlWithParamToDisplay?.paramToDisplay
        ? urlObject.searchParams.getAll(urlWithParamToDisplay.paramToDisplay)
        : [];
    const hasParams = paramIds.length > 0;

    const content = (
        <div className="hstack w-100 gap-1 d-inline-flex align-items-center">
            <span className="text-nowrap">
                <EndpointItemStateIcon state={endpoint.state} />
                <span className="font-monospace fw-semibold font-size-12">GET</span>
            </span>
            <a href={endpoint.url} target="_blank" className="text-truncate no-decor" title={endpoint.url}>
                {urlWithParamToDisplay ? urlObject.pathname : endpoint.url}
            </a>
            {endpoint.resultSizeInBytes != null && (
                <ConditionalPopover
                    conditions={{
                        isActive: endpoint.isRequestTooLarge,
                        message: "Request too large to process",
                    }}
                    className="ms-auto"
                >
                    <Badge className="text-nowrap" bg={endpoint.isRequestTooLarge ? "danger" : "secondary"} pill>
                        {genUtils.formatBytesToSize(endpoint.resultSizeInBytes)}
                    </Badge>
                </ConditionalPopover>
            )}
        </div>
    );

    if (hasParams) {
        return (
            <li className="branch">
                {content}
                <ul>
                    {paramIds.map((id) => (
                        <li key={id} className="leaf">
                            <span className="d-block text-truncate" title={id}>
                                {id}
                            </span>
                        </li>
                    ))}
                </ul>
            </li>
        );
    }

    return <li className="leaf">{content}</li>;
}

function EndpointItemStateIcon({ state }: Pick<ChatbotEndpointItem, "state">) {
    switch (state) {
        case "allowed":
        case "alwaysAllowed":
            return <Icon icon="check" color="success" />;
        case "error":
            return <Icon icon="warning" color="danger" />;
        case "skipped":
            return <Icon icon="skip" />;
        case "denied":
            return <Icon icon="cancel" />;
        case "waiting":
        default:
            return null;
    }
}

function isOnWhitelist(url: string) {
    return chatbotConstants.whitelistRegexEndpoints.some(({ regex }) =>
        regex.test(new URL(url, window.location.origin).pathname)
    );
}

function isWithDataSubmission(url: string) {
    return chatbotConstants.dataSubmissionRegexEndpoints.some(({ regex }) =>
        regex.test(new URL(url, window.location.origin).pathname)
    );
}

function getExposedFieldsConfig(url: string): ChatbotEndpointEntry["exposedFieldsConfig"] {
    const endpoint = chatbotConstants.exposingFieldsRegexEndpoints.find(({ regex }) =>
        regex.test(new URL(url, window.location.origin).pathname)
    );

    return endpoint?.exposedFieldsConfig;
}

function createExposedFieldsResult(
    jsonData: any,
    exposedFieldsConfig: ChatbotEndpointEntry["exposedFieldsConfig"]
): Pick<EndpointResult, "resultText" | "resultSizeInBytes"> {
    const filteredData: Record<string, any> = {};

    if (exposedFieldsConfig.resultShape === "singleObject") {
        for (const field of exposedFieldsConfig.fields) {
            if (field in jsonData) {
                filteredData[field] = jsonData[field];
            }
        }
    } else if (exposedFieldsConfig.resultShape === "resultsArray") {
        filteredData.Results = jsonData.Results.map((item: any) => {
            const filteredItem: Record<string, any> = {};
            for (const field of exposedFieldsConfig.fields) {
                if (field in item) {
                    filteredItem[field] = item[field];
                }
            }
            return filteredItem;
        });
    }

    const resultText = JSON.stringify(filteredData);
    const resultSizeInBytes = new Blob([resultText]).size;

    return { resultText, resultSizeInBytes };
}
