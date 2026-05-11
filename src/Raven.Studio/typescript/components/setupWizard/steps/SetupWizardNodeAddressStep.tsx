import { Control, useFieldArray, useForm, useFormContext, UseFormReturn, useWatch } from "react-hook-form";
import { SetupWizardFormData, SetupWizardSecurityOption } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import Collapse from "react-bootstrap/Collapse";
import React, { useEffect, useMemo } from "react";
import Form from "react-bootstrap/Form";
import {
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSwitch,
    OptionalLabel,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import useConfirm from "components/common/ConfirmDialog";
import InputGroup from "react-bootstrap/InputGroup";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import classNames from "classnames";
import genUtils from "common/generalUtils";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { SelectOption } from "components/common/select/Select";
import { isEmpty } from "common/typeUtils";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/esm/Col";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardConstants, setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import useBoolean from "hooks/useBoolean";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { components, OptionProps, SingleValueProps } from "react-select";
import Badge from "react-bootstrap/Badge";

export function SetupWizardNodeAddressStep() {
    const { control } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();
    const { setupWizardService } = useServices();
    const { fields, append, remove } = useFieldArray({
        control,
        name: "nodeAddressStep.nodes",
    });

    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);

    const {
        domainStep,
        selfSignedCertificateStep: { cns, isWildcardCertificate },
        securityStep: { securityOption },
    } = useWatch({ control });

    const fullDomain = `a.${domainStep.domain.toLocaleLowerCase()}.${domainStep.rootDomain}`;

    const getDomainForWildcard = (tag: string | null): string => {
        if (cns.length === 0) {
            return null;
        }

        const cn = cns[0];

        if (!tag) {
            return cn.replace("*.", "");
        }
        return cn.replace("*", tag);
    };

    const handleDefaultNodeUrlConfiguration = () => {
        if (securityOption === "letsEncrypt") {
            return fullDomain;
        }

        if (securityOption === "ownCertificate") {
            if (isWildcardCertificate) {
                return getDomainForWildcard(null);
            } else {
                return cns[0];
            }
        }

        return null;
    };

    const addNewNode = () => {
        const existingTags = fields.map((field) => field.nodeTag);
        const nodeTags = generateAlphabeticTags(2); // validation allows us to contain 4 letters, but generated tags are around +- 500k length, so we limit it to 2. (700 tags)
        const availableNodeTag = nodeTags.find((tag) => !existingTags.includes(tag)) || "A";

        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "add-node", availableNodeTag);

        append({
            nodeTag: availableNodeTag,
            ipAddress: [
                {
                    ipAddress: asyncGetSetupParameters.result?.IsDocker ? "" : "127.0.0.1",
                },
            ],
            dnsName: securityOption === "ownCertificate" && !isWildcardCertificate ? cns[0] : undefined,
            isEditing: true,
            isNewlyAdded: true,
        });
    };

    // add default node
    useEffect(() => {
        if (fields.length === 0) {
            append({
                nodeTag: "A",
                ipAddress: [
                    {
                        ipAddress: asyncGetSetupParameters.result?.IsDocker ? "" : "127.0.0.1",
                    },
                ],
                dnsName: securityOption === "ownCertificate" && !isWildcardCertificate ? cns[0] : undefined,
                isEditing: true, // the first node should be added with default values and in editing mode
                isNewlyAdded: false,
                isPassive: false,
                nodeUrl: handleDefaultNodeUrlConfiguration(),
                httpPort: securityOption === "none" ? 8080 : 443,
                tcpPort: 38888,
                hasExternalConfig: false,
            });
        }
    }, []); // eslint-disable-line react-hooks/exhaustive-deps

    useRevalidatePersistedNodesOnEntry();

    return (
        <div>
            <div className="mb-4">
                <h2 className="mb-1">Node addresses</h2>
                <p className="mb-4 text-muted">
                    Configure the cluster by adding nodes and setting their network details.
                    <br />
                    For each node, specify its tag name, ports, and IP address or host name.
                </p>
            </div>
            <div className="vstack">
                {fields.map((field, index) => (
                    <NodeDetailsPanel key={field.id} control={control} index={index} onRemove={() => remove(index)} />
                ))}
                <AddAnotherNode onAddNode={addNewNode} />
            </div>
        </div>
    );
}

function generateAlphabeticTags(maxLength: number = 4): string[] {
    const tags: string[] = [];
    let currentTag = "";

    while (currentTag.length <= maxLength) {
        tags.push(getNextTag(currentTag));
        currentTag = getNextTag(currentTag);
    }

    return tags;
}

function getNextTag(current: string): string {
    if (!current) {
        return "A";
    }

    const chars = current.split("");
    let i = chars.length - 1;

    while (i >= 0) {
        if (chars[i] !== "Z") {
            chars[i] = String.fromCharCode(chars[i].charCodeAt(0) + 1);
            break;
        }
        chars[i] = "A";
        i--;
    }

    if (i < 0) {
        chars.unshift("A");
    }

    return chars.join("");
}

interface NodeDetailsPanelProps {
    control: Control<SetupWizardFormData>;
    index: number;
    onRemove: () => void;
}

function NodeDetailsPanel({ control, index, onRemove }: NodeDetailsPanelProps) {
    const { getValues } = useFormContext<SetupWizardFormData>();
    const nodeData = useWatch({
        control,
        name: `nodeAddressStep.nodes.${index}`,
    });

    const {
        securityStep: { securityOption },
        selfSignedCertificateStep: { cns, isWildcardCertificate },
    } = useWatch({
        control,
    });
    const { setupWizardService } = useServices();

    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);

    const nodeAddressStep = getValues().nodeAddressStep;

    const editNodeForm = useForm<NodeEditFormData>({
        defaultValues: nodeData,
        mode: "onChange",
        resolver: yupResolver(nodeEditFormSchema),
        context: {
            nodeAddressStep,
            securityOption,
            currentIndex: index,
            isDocker: asyncGetSetupParameters.result?.IsDocker,
            cns,
            isWildcardCertificate,
        },
    });

    return (
        <RichPanel hover>
            <NodeDetailsPanelHeader control={control} editNodeForm={editNodeForm} index={index} onRemove={onRemove} />
            {!nodeData.isEditing ? (
                <NodeDetailsPanelView index={index} control={control} />
            ) : (
                <NodeDetailsPanelEdit parentControl={control} editNodeForm={editNodeForm} />
            )}
        </RichPanel>
    );
}

interface NodeDetailsPanelHeaderProps {
    control: Control<SetupWizardFormData>;
    index: number;
    onRemove: () => void;
    editNodeForm: UseFormReturn<NodeEditFormData>;
}

function NodeDetailsPanelHeader({ control, index, onRemove, editNodeForm }: NodeDetailsPanelHeaderProps) {
    const { setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();
    const nodeData = useWatch({
        control,
        name: `nodeAddressStep.nodes.${index}`,
    });

    const {
        domainStep,
        selfSignedCertificateStep: { isWildcardCertificate, cns },
        setupMethodStep: { method },
        nodeAddressStep: { nodes },
        securityStep: { securityOption },
    } = useWatch({
        control,
    });

    const { handleSubmit, reset, formState } = editNodeForm;

    const nodeName = `Node ${nodeData.nodeTag ?? "?"}`;

    const handleDiscardEdit = () => {
        reportEvent(
            setupWizardGA4Prefixes.nodeAddressStep,
            "discard-edit",
            nodeData.isNewlyAdded ? "new" : (nodeData.nodeTag ?? "?")
        );
        if (nodeData.isNewlyAdded) {
            onRemove();
        } else {
            reset(nodeData);
            setValue(`nodeAddressStep.nodes.${index}`, {
                ...nodeData,
                isEditing: false,
            });
        }
    };

    const handleNodeUrl = (formData: NodeEditFormData) => {
        if (securityOption === "letsEncrypt") {
            return `${formData.nodeTag.toLowerCase()}.${domainStep.domain.toLocaleLowerCase()}.${domainStep.rootDomain}`;
        }

        if (securityOption === "ownCertificate") {
            let nodeUrl: string;

            if (isWildcardCertificate) {
                // For wildcard certificates, construct domain from CN and node tag
                if (cns.length > 0 && cns[0].includes("*")) {
                    nodeUrl = cns[0].replace("*", formData.nodeTag.toLowerCase());
                } else {
                    // Fallback to dnsName if available
                    nodeUrl = formData.dnsName || "";
                }
            } else {
                // For non-wildcard certificates, use the selected dnsName
                nodeUrl = formData.dnsName || "";
            }

            if (formData.httpPort !== 443 && formData.httpPort != null) {
                nodeUrl += ":" + formData.httpPort;
            }
            return nodeUrl;
        }

        return null;
    };

    const handleSaveEdit = handleSubmit(async (formData: NodeEditFormData) => {
        setValue(`nodeAddressStep.nodes.${index}`, {
            ...formData,
            dnsName: securityOption === "ownCertificate" ? formData.dnsName : null,
            nodeUrl: handleNodeUrl(formData),
            httpPort: formData.httpPort == null ? (securityOption === "none" ? 8080 : 443) : formData.httpPort,
            isEditing: false,
            isNewlyAdded: false,
        });
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "save-node", formData.nodeTag ?? "");
    });

    const confirm = useConfirm();

    const handleDeleteNode = async () => {
        const isConfirmed = await confirm({
            title: (
                <>
                    You’re about to delete <b>Node {nodeData.nodeTag}</b>
                </>
            ),
            message: (
                <div className="d-flex w-100 justify-content-center">
                    Removing it may impact cluster stability and performance. This action cannot be undone.
                </div>
            ),
            icon: "trash",
            confirmText: "Delete",
            actionColor: "danger",
            size: "lg",
        });

        if (isConfirmed) {
            reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "remove-node", nodeData.nodeTag ?? "?");
            onRemove();
        }
    };

    const isPassiveVisible = method !== "createPackage" && nodes.length === 1;

    return (
        <RichPanelHeader>
            <RichPanelInfo>
                <RichPanelName size="sm">
                    {nodeData.isNewlyAdded ? (
                        <>Creating new node</>
                    ) : nodeData.isEditing ? (
                        <>Editing node values</>
                    ) : (
                        <>
                            <Icon color="node" icon="node" />
                            {nodeName}{" "}
                            {index === 0 && (
                                <small className="text-muted">
                                    {nodeData.isPassive && isPassiveVisible ? (
                                        <span className="text-info">(Passive)</span>
                                    ) : (
                                        "(current node)"
                                    )}
                                </small>
                            )}
                        </>
                    )}
                </RichPanelName>
            </RichPanelInfo>
            <RichPanelActions>
                {nodeData.isEditing ? (
                    <>
                        <ConditionalPopover
                            conditions={{
                                isActive: !isEmpty(formState.errors),
                                message: "Please fix the errors before saving.",
                            }}
                        >
                            <Button disabled={!isEmpty(formState.errors)} onClick={handleSaveEdit} variant="success">
                                <Icon icon="save" />
                                Save
                            </Button>
                        </ConditionalPopover>
                        <Button variant="secondary" onClick={handleDiscardEdit}>
                            {nodeData.isNewlyAdded ? (
                                <>
                                    <Icon icon="trash" />
                                    Remove
                                </>
                            ) : (
                                <>
                                    <Icon icon="close" />
                                    Discard
                                </>
                            )}
                        </Button>
                    </>
                ) : (
                    <>
                        <Button
                            variant="secondary"
                            onClick={() => {
                                reportEvent(
                                    setupWizardGA4Prefixes.nodeAddressStep,
                                    "edit-node",
                                    nodeData.nodeTag ?? "?"
                                );
                                setValue(`nodeAddressStep.nodes.${index}.isEditing`, true);
                            }}
                        >
                            <Icon icon="edit" margin="m-0" />
                        </Button>
                        {nodes.filter((node) => !node.isNewlyAdded).length > 1 && (
                            <Button variant="danger" onClick={handleDeleteNode}>
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </>
                )}
            </RichPanelActions>
        </RichPanelHeader>
    );
}

interface NodeDetailsPanelViewProps {
    index: number;
    control: Control<SetupWizardFormData>;
}

function NodeDetailsPanelView({ index, control }: NodeDetailsPanelViewProps) {
    const setupWizardData = useWatch({
        control,
    });

    const nodeData = setupWizardData.nodeAddressStep.nodes[index];

    const localIpPortAddress = `${nodeData.ipAddress[0].ipAddress}:${nodeData.httpPort}`;
    return (
        <RichPanelDetails>
            <RichPanelDetailItem>
                <div className="d-flex flex-column w-100">
                    <span className="hstack">
                        <span className="md-label mb-0">Node URL</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Defines the address under which specific node will be available."
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div className="text-truncate" title={nodeData.nodeUrl || localIpPortAddress}>
                        {nodeData.nodeUrl || localIpPortAddress}
                    </div>
                </div>
            </RichPanelDetailItem>

            {nodeData.dnsName && (
                <RichPanelDetailItem>
                    <div className="d-flex flex-column w-100">
                        <span className="hstack">
                            <span className="md-label mb-0">DNS Name</span>
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description="Defines the address under which specific node will be available."
                                        ravenLinkHash="MGIZZM"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                        <div className="text-truncate" title={nodeData.dnsName}>
                            {nodeData.dnsName}
                        </div>
                    </div>
                </RichPanelDetailItem>
            )}

            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">
                            {setupWizardData.securityStep.securityOption === "none" ? "HTTP" : "HTTPS"} port
                        </span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={`Defines the communication endpoint for clients and browsers. By default, this value is set to ${setupWizardData.securityStep.securityOption === "none" ? "8080" : "443"}.`}
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div>
                        {nodeData.hasExternalConfig && nodeData.externalHttpPort
                            ? nodeData.externalHttpPort
                            : nodeData.httpPort}
                    </div>
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">TCP port</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Defines the TCP endpoint for cluster nodes to communicate with each other. By default, this value is set to 38888."
                                    ravenLinkHash="9D6HG1"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    <div>
                        {nodeData.hasExternalConfig && nodeData.externalTcpPort
                            ? nodeData.externalTcpPort
                            : nodeData.tcpPort}
                    </div>
                </div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                <div className="d-flex flex-column">
                    <span className="hstack">
                        <span className="md-label mb-0">IP address/Hostname</span>
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Defines the network endpoint where the server is accessible."
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    {nodeData.hasExternalConfig && nodeData.externalIpAddress ? (
                        <div>{nodeData.externalIpAddress}</div>
                    ) : (
                        <div>{nodeData.ipAddress.map((x) => x.ipAddress).join(", ")}</div>
                    )}
                </div>
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
}

function NodeDetailsPanelEdit({
    editNodeForm,
    parentControl,
}: {
    editNodeForm: UseFormReturn<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}) {
    const { control } = editNodeForm;
    const nodeData = useWatch({
        control,
    });

    const {
        domainStep,
        securityStep: { securityOption },
        setupMethodStep: { method: setupMethod },
        selfSignedCertificateStep: { cns, isWildcardCertificate },
        nodeAddressStep: { nodes },
    } = useWatch({
        control: parentControl,
    });

    const { isExternalRequired } = useHostnameDetectionSideEffects({ editNodeForm, parentControl });

    const isDNSVisible = securityOption === "ownCertificate" && !isWildcardCertificate;
    const isPassiveVisible = setupMethod !== "createPackage" && nodes.length === 1;

    const canCustomizeExternalIpsAndPorts = securityOption === "letsEncrypt";
    const canCustomizeExternalTcpPorts = securityOption === "ownCertificate";

    const colWidth = isDNSVisible ? 6 : 4;

    const shouldDisplayUnsafeMode = nodeData.ipAddress.some(
        ({ ipAddress }) => !genUtils.isLocalhostIpAddress(ipAddress)
    );

    return (
        <RichPanelDetails>
            <Form className="w-100 p-2">
                {isPassiveVisible && (
                    <FormGroup>
                        <FormSwitch name="isPassive" control={control}>
                            <span className="hstack">
                                Start node as Passive, not part of a cluster
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description="When enabled, the node starts in passive mode and does not join a cluster. 
                                                This is useful when the node is meant for monitoring, initialization, or handling setup tasks without participating in cluster operations. 
                                                It can also be used to isolate the node for testing or debugging."
                                            ravenLinkHash="2WV7N1"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormSwitch>
                    </FormGroup>
                )}
                <Row>
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                Node tag
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description="Defines a unique identifier for each node in the cluster."
                                            alert={
                                                <RichAlert variant="info" icon="info" className="mt-1">
                                                    Node tag can contain a maximum of 4 uppercase letters (A-Z).
                                                </RichAlert>
                                            }
                                            ravenLinkHash="WJJHFY"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                disabled={nodeData.isPassive && isPassiveVisible}
                                placeholder={
                                    nodeData.isPassive && isPassiveVisible
                                        ? "Node will start in Passive state"
                                        : "Enter Node Tag"
                                }
                                type="text"
                                name="nodeTag"
                                control={control}
                            />
                        </FormGroup>
                    </Col>
                    {isDNSVisible && (
                        <Col md={colWidth}>
                            <FormGroup>
                                <FormLabel className="d-flex">
                                    DNS Name
                                    <PopoverWithHoverWrapper
                                        message={
                                            <SetupWizardInfoPopover
                                                description={
                                                    <>
                                                        Domain name that will be used to reach the server on this node.
                                                        <br />
                                                        Note: It must be associated with the chosen IP Address below.
                                                    </>
                                                }
                                            />
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </FormLabel>
                                <FormSelect
                                    name="dnsName"
                                    control={control}
                                    placeholder="Select DNS Name"
                                    options={cns.map((x) => ({ value: x, label: x }))}
                                />
                            </FormGroup>
                        </Col>
                    )}
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                {securityOption === "none" ? "HTTP" : "HTTPS"} port
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description={`Defines the private ${securityOption === "none" ? "HTTP" : "HTTPS"} port used by clients and browsers. By default, this value is set to ${securityOption === "none" ? "8080" : "443"}.`}
                                            ravenLinkHash="MGIZZM"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                type="number"
                                name="httpPort"
                                placeholder={securityOption === "none" ? "Default: 8080" : "Default: 443"}
                                control={control}
                            />
                        </FormGroup>
                    </Col>
                    <Col md={colWidth}>
                        <FormGroup>
                            <FormLabel className="d-flex">
                                TCP Port
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description="Defines the TCP port used for internal communication between cluster nodes.
                                                By default, this value is set to 38888."
                                            ravenLinkHash="9D6HG1"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput type="number" name="tcpPort" placeholder="Default: 38888" control={control} />
                        </FormGroup>
                    </Col>
                </Row>
                <IpAddressList parentControl={parentControl} control={control} />
                <div className="d-flex flex-column gap-1">
                    {securityOption === "letsEncrypt" && nodeData.ipAddress.length > 0 && (
                        <RichAlert variant="info" icon="info">
                            RavenDB will update the DNS record for{" "}
                            <span className="text-decoration-underline">{`${nodeData.nodeTag.toLowerCase()}.${domainStep.domain.toLowerCase()}.${domainStep.rootDomain}`}</span>{" "}
                            to IP{" "}
                            {nodeData.ipAddress.length > 1 && !nodeData.externalIpAddress ? "addresses" : "address"}:{" "}
                            {nodeData.externalIpAddress && nodeData.hasExternalConfig ? (
                                <span className="text-decoration-underline">{nodeData.externalIpAddress}</span>
                            ) : nodeData.ipAddress.length > 0 ? (
                                <span className="text-decoration-underline">
                                    {nodeData.ipAddress.map((x) => x.ipAddress).join(", ")}
                                </span>
                            ) : (
                                <span className="text-decoration-underline">&lt;insert IP addresses&gt;</span>
                            )}
                        </RichAlert>
                    )}
                    {securityOption === "none" && shouldDisplayUnsafeMode && (
                        <RichAlert variant="warning" icon="warning">
                            Node IP is not configured for local network. By proceeding, you admit that you understand
                            the risk behind running RavenDB server in the Unsecure mode. Authentication is off, so
                            anyone who can access this IP will be granted <i>administrative privileges</i>.
                        </RichAlert>
                    )}
                </div>

                {(canCustomizeExternalIpsAndPorts || canCustomizeExternalTcpPorts) && (
                    <FormSwitch
                        name="hasExternalConfig"
                        color="primary"
                        disabled={isExternalRequired}
                        control={control}
                        className="mt-3 mb-2"
                    >
                        <span className="d-flex">
                            Customize external IP and ports
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description="External overrides allow you to specify an alternative IP address, hostname, 
                                            or HTTPS port that clients will use instead of the server’s default settings."
                                        ravenLinkHash="Z112DU"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormSwitch>
                )}
                <Collapse in={nodeData.hasExternalConfig}>
                    <Row>
                        <EditFormExternalAddressInputs
                            control={control}
                            canCustomizeExternalIpsAndPorts={canCustomizeExternalIpsAndPorts}
                            canCustomizeExternalTcpPorts={canCustomizeExternalTcpPorts}
                        />
                    </Row>
                </Collapse>
            </Form>
        </RichPanelDetails>
    );
}

function EditFormExternalAddressInputs({
    control,
    canCustomizeExternalIpsAndPorts,
    canCustomizeExternalTcpPorts,
}: {
    control: Control<NodeEditFormData>;
    canCustomizeExternalIpsAndPorts: boolean;
    canCustomizeExternalTcpPorts: boolean;
}) {
    return (
        <>
            <Col>
                <FormGroup className="vstack w-100">
                    <FormLabel>
                        <span className="d-flex">
                            External IP address
                            <PopoverWithHoverWrapper
                                message={
                                    <SetupWizardInfoPopover
                                        description="Defines the public IP address from which requests will be
                                            forwarded to the private IP address that RavenDB listens on."
                                        ravenLinkHash="Z112DU"
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </span>
                    </FormLabel>
                    <FormInput
                        type="text"
                        name="externalIpAddress"
                        placeholder="Enter Server IP address/hostname"
                        control={control}
                    />
                </FormGroup>
            </Col>
            {canCustomizeExternalIpsAndPorts && (
                <Col>
                    <FormGroup className="vstack w-100">
                        <FormLabel>
                            <span className="d-flex align-items-baseline">
                                <span>External HTTPS port&nbsp;</span> <OptionalLabel />
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description="Defines the public HTTPS port that clients and browsers will use
                                                instead of the default binding."
                                            ravenLinkHash="Z112DU"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormLabel>
                        <FormInput
                            type="number"
                            name="externalHttpPort"
                            placeholder="Enter external HTTPS port"
                            control={control}
                        />
                    </FormGroup>
                </Col>
            )}
            {(canCustomizeExternalIpsAndPorts || canCustomizeExternalTcpPorts) && (
                <Col>
                    <FormGroup className="vstack w-100">
                        <FormLabel>
                            <span className="d-flex align-items-baseline">
                                <span>External TCP Port&nbsp;</span>
                                <OptionalLabel />
                                <PopoverWithHoverWrapper
                                    message={
                                        <SetupWizardInfoPopover
                                            description="Defines the public TCP port used for inter-node communication
                                                and client connections."
                                            ravenLinkHash="Z112DU"
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </span>
                        </FormLabel>
                        <FormInput
                            type="number"
                            name="externalTcpPort"
                            placeholder="Enter external TCP port"
                            control={control}
                        />
                    </FormGroup>
                </Col>
            )}
        </>
    );
}

interface AddAnotherNodeProps {
    onAddNode: () => void;
}

function AddAnotherNode({ onAddNode }: AddAnotherNodeProps) {
    const { getValues } = useFormContext<SetupWizardFormData>();

    const licenseKeyStep = getValues("licenseKeyStep");
    const nodeData = getValues("nodeAddressStep.nodes");

    const hasLicense = !!licenseKeyStep?.licenseInfo;
    const maxClusterSize = licenseKeyStep?.licenseInfo?.maxClusterSize ?? setupWizardConstants.AGPL_MAX_CLUSTER_SIZE;
    const isMaxClusterNodes = nodeData?.length >= maxClusterSize;

    return (
        <div
            className={classNames(
                "mt-3 w-100 d-flex rounded justify-content-center mb-2 align-items-center border-dashed border-2",
                isMaxClusterNodes ? "border-secondary" : "border-node"
            )}
        >
            <ConditionalPopover
                conditions={[
                    {
                        isActive: !hasLicense,
                        message: (
                            <>
                                <p className="mb-0">
                                    Without a license you can only run a single-node cluster. Otherwise, go back to the{" "}
                                    <b>License Key</b> step to provide a license and unlock multi-node clusters.
                                </p>
                                <hr className="my-2" />
                                <span className="md-label">
                                    <Icon icon="link" /> See{" "}
                                    <a href="https://ravendb.net/buy" target="_blank">
                                        licenses comparison <Icon icon="newtab" />
                                    </a>
                                </span>
                            </>
                        ),
                    },
                    {
                        isActive: isMaxClusterNodes,
                        message: (
                            <>
                                <p className="mb-0">Your license doesn&apos;t allow more nodes in the cluster.</p>
                                <hr className="my-2" />
                                <span className="md-label">
                                    <Icon icon="link" /> See{" "}
                                    <a href="https://ravendb.net/buy" target="_blank">
                                        licenses comparison <Icon icon="newtab" />
                                    </a>
                                </span>
                            </>
                        ),
                    },
                ]}
            >
                <Button
                    disabled={isMaxClusterNodes}
                    variant={isMaxClusterNodes ? "outline-secondary" : "outline-node"}
                    className={classNames("rounded-pill my-4", {
                        "item-disabled": isMaxClusterNodes,
                    })}
                    onClick={onAddNode}
                >
                    <Icon icon="node-add" />
                    Add another node
                </Button>
            </ConditionalPopover>
        </div>
    );
}

interface UseHostnameDetectionSideEffectsProps {
    editNodeForm: UseFormReturn<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}

function useHostnameDetectionSideEffects({ editNodeForm, parentControl }: UseHostnameDetectionSideEffectsProps) {
    const { setupWizardService } = useServices();
    const asyncGetSetupParameters = useAsync(async () => setupWizardService.getSetupParameters(), []);
    const { setValue, control, watch, clearErrors } = editNodeForm;
    const { value: wasAutoEnabled, setValue: setWasAutoEnabled } = useBoolean(false);
    const { value: prevHadBindAllIp, setValue: setPrevHadBindAllIp } = useBoolean(false);

    const nodeData = useWatch({ control });
    const {
        securityStep: { securityOption },
        nodeAddressStep: { nodes },
    } = useWatch({ control: parentControl });

    const hasBindAllIp = useMemo(
        () => nodeData.ipAddress.some((ip) => genUtils.isBindAllIpAddress(ip.ipAddress)),
        [nodeData.ipAddress]
    );

    const ipsContainHostname = useMemo(
        () => nodeData.ipAddress.some((ip) => genUtils.isHostname(ip.ipAddress)),
        [nodeData.ipAddress]
    );

    const isHostname = useMemo(
        () => ipsContainHostname && securityOption === "ownCertificate",
        [ipsContainHostname, securityOption]
    );

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            const { hasExternalConfig, ipAddress, isPassive, nodeTag } = values;

            const containsHostname = ipAddress.some((ip) => genUtils.isHostname(ip.ipAddress));
            const containsBindAllIp = ipAddress.some((ip) => genUtils.isBindAllIpAddress(ip.ipAddress));

            if (name === "hasExternalConfig") {
                setWasAutoEnabled(false);

                if (!hasExternalConfig) {
                    setValue("externalIpAddress", "");
                    clearErrors("externalIpAddress");
                }

                return;
            }

            if (isPassive && nodes.length > 1) {
                setValue("isPassive", false);
            }

            if (isPassive && nodes.length > 1 && nodeTag) {
                setValue("nodeTag", "", { shouldValidate: false });
            }

            const shouldAutoDisable =
                !containsBindAllIp &&
                hasExternalConfig &&
                !containsHostname &&
                !asyncGetSetupParameters.result?.IsDocker &&
                wasAutoEnabled &&
                prevHadBindAllIp &&
                securityOption === "letsEncrypt";

            if (shouldAutoDisable) {
                setValue("hasExternalConfig", false, { shouldValidate: true });
                clearErrors("externalIpAddress");
                setWasAutoEnabled(false);
            }

            const shouldAutoEnable =
                securityOption === "letsEncrypt" && (containsHostname || containsBindAllIp) && !hasExternalConfig;

            if (shouldAutoEnable) {
                setValue("hasExternalConfig", true, { shouldValidate: true });
                setWasAutoEnabled(true);
            }

            setPrevHadBindAllIp(containsBindAllIp);
        });

        return () => unsubscribe();
    }, [watch, setValue, securityOption]); // eslint-disable-line react-hooks/exhaustive-deps

    return {
        isHostname,
        isExternalRequired: securityOption !== "ownCertificate" && (isHostname || hasBindAllIp || ipsContainHostname),
    };
}

function IpAddressList({
    control,
    parentControl,
}: {
    control: Control<NodeEditFormData>;
    parentControl: Control<SetupWizardFormData>;
}) {
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { append, remove, fields } = useFieldArray<NodeEditFormData>({
        control,
        name: "ipAddress",
    });

    const {
        securityStep: { securityOption },
    } = useWatch({ control: parentControl });

    const addIpAddress = () => {
        append({ ipAddress: "" });
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "add-ip");
    };

    const asyncGetSetupLocalNodeIps = useAsync(async () => setupWizardService.getSetupLocalNodeIps(), []);

    const localIpAddresses: SelectOption[] = Array.from(new Set(asyncGetSetupLocalNodeIps.result || [])).map((ip) => ({
        label: ip,
        value: ip,
    }));

    const ipAddressesOptions: SelectOption[] = [{ label: "0.0.0.0", value: "0.0.0.0" }, ...localIpAddresses];

    return (
        <FormGroup className="w-100">
            <FormLabel className="w-100">
                <div className="hstack justify-content-between">
                    <span className="d-flex">
                        IP address/Hostname
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description="Defines the IP address or hostname used to access the server."
                                    ravenLinkHash="MGIZZM"
                                />
                            }
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </span>
                    {securityOption !== "none" && (
                        <Button
                            variant="link"
                            size="xs"
                            onClick={addIpAddress}
                            title="Add more IP Addresses or hostnames"
                            className="px-0"
                        >
                            <Icon icon="plus" />
                            Add another IP Address
                        </Button>
                    )}
                </div>
            </FormLabel>
            <div className="vstack gap-2">
                {fields.map((field, ipIndex) => (
                    <InputGroup key={field.id}>
                        <FormSelectAutocomplete
                            control={control}
                            placeholder="Enter Server IP A address/hostname"
                            isLoading={asyncGetSetupLocalNodeIps.loading}
                            name={`ipAddress.${ipIndex}.ipAddress`}
                            options={ipAddressesOptions}
                            components={{
                                Option: IpAddressOptionComponent,
                                SingleValue: IpAddressSingleValueComponent,
                            }}
                        />
                        {ipIndex > 0 && (
                            <Button
                                variant="outline-danger"
                                size="sm"
                                onClick={() => {
                                    reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "remove-ip");
                                    remove(ipIndex);
                                }}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </InputGroup>
                ))}
            </div>
        </FormGroup>
    );
}

function IpAddressOptionComponent(props: OptionProps<SelectOption>) {
    const { data } = props;

    return (
        <components.Option {...props} className={classNames(props.className, "hstack")}>
            {data.label}
            <NoRemoteAccessBadge ipAddress={data.value} />
        </components.Option>
    );
}

function IpAddressSingleValueComponent({ children, ...props }: SingleValueProps<SelectOption>) {
    return (
        <components.SingleValue {...props} className={classNames(props.className, "hstack")}>
            {children}
            <NoRemoteAccessBadge ipAddress={props.data.value} />
        </components.SingleValue>
    );
}

function NoRemoteAccessBadge({ ipAddress }: { ipAddress: string }) {
    const isLocalhostIpAddress = genUtils.isLocalhostIpAddress(ipAddress);

    if (!isLocalhostIpAddress) {
        return null;
    }

    return (
        <Badge bg="secondary" className="ms-1" pill>
            No remote access
        </Badge>
    );
}

export function SetupWizardNodeAddressStepFooter() {
    const { setValue, getValues } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const nodeData = getValues("nodeAddressStep.nodes");
    const licenseKeyStep = getValues("licenseKeyStep");

    const maxClusterSize = licenseKeyStep?.licenseInfo?.maxClusterSize ?? setupWizardConstants.AGPL_MAX_CLUSTER_SIZE;
    const hasExceededLicenseLimit = nodeData?.length > maxClusterSize;

    const isEditing = nodeData?.some((node) => node.isEditing);

    const confirm = useConfirm();

    const areAllNodesIdentical = (() => {
        if (!nodeData || nodeData.length <= 1) {
            return false;
        }

        const firstNode = nodeData[0];

        const firstNodeIps = firstNode.ipAddress
            .map((ip) => ip.ipAddress)
            .sort()
            .join(",");
        const firstNodeTcpPort = firstNode.tcpPort;
        const firstNodeHttpPort = firstNode.httpPort;

        return nodeData.every((node) => {
            const nodeIps = node.ipAddress
                .map((ip) => ip.ipAddress)
                .sort()
                .join(",");

            return nodeIps === firstNodeIps && node.tcpPort === firstNodeTcpPort && node.httpPort === firstNodeHttpPort;
        });
    })();

    const hasDuplicateNodeConfigurations = (() => {
        const configurationMap = new Map();

        for (const node of nodeData) {
            for (const ip of node.ipAddress) {
                const configKey = `${ip.ipAddress}-${node.httpPort}-${node.tcpPort}`;

                if (configurationMap.has(configKey)) {
                    return true;
                }

                configurationMap.set(configKey, true);
            }
        }

        return false;
    })();

    const handleContinue = async () => {
        const nodeCount = nodeData.length;

        if (areAllNodesIdentical) {
            const firstNode = nodeData[0];
            const isConfirmed = await confirm({
                title: "Identical node configurations",
                message: (
                    <>
                        <span>All nodes in the cluster are configured with the same settings:</span>
                        <ul>
                            <li>IP address: {firstNode.ipAddress[0]?.ipAddress}</li>
                            <li>TCP port: {firstNode.tcpPort}</li>
                            <li>HTTPS port: {firstNode.httpPort}</li>
                        </ul>
                        If you plan to deploy these nodes on a single machine, it will cause port conflict.
                        <br />
                        Please confirm that these settings are correct before proceeding to the next step.
                    </>
                ),
                icon: "warning",
                confirmText: "Proceed",
                actionColor: "warning",
                size: "lg",
            });

            if (!isConfirmed) {
                return;
            }
        } else if (hasDuplicateNodeConfigurations) {
            const isConfirmed = await confirm({
                title: "Duplicate node configurations",
                message:
                    "You have multiple nodes with identical IP address, TCP port, and HTTPS port configurations. " +
                    "This may cause conflicts in your cluster. Are you sure you want to proceed?",
                icon: "warning",
                confirmText: "Proceed anyway",
                actionColor: "warning",
                size: "lg",
            });

            if (!isConfirmed) {
                return;
            }
        }

        // Then check for even node count
        if (nodeCount % 2 === 0) {
            const isConfirmed = await confirm({
                title: "Confirm even node count",
                message: `You've chosen an even number of nodes for your cluster. For optimal replication and high availability, an odd number of nodes is usually recommended.
                        Are you sure you want to proceed with an even node count?`,
                icon: "warning",
                confirmText: "Proceed",
                actionColor: "warning",
                size: "lg",
            });

            if (isConfirmed) {
                reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "continue", nodeCount.toString());
                setValue("currentStep", "Additional settings");
            }
        } else {
            reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "continue", nodeCount.toString());
            setValue("currentStep", "Additional settings");
        }
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.nodeAddressStep, "back");
        const setupWizardFormData = getValues();
        switch (setupWizardFormData.securityStep.securityOption) {
            case "letsEncrypt":
                setValue("currentStep", "Domain");
                break;
            case "ownCertificate":
                setValue("currentStep", "Self-signed certificate");
                break;
            case "none":
                setValue("currentStep", "Security");
                break;
        }
    };

    const isContinueDisabled = isEditing || hasExceededLicenseLimit || nodeData.length === 0;

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> Back
            </Button>
            <ConditionalPopover
                conditions={[
                    {
                        isActive: hasExceededLicenseLimit,
                        message: <LicenseLimitExceededMessage />,
                    },
                    {
                        isActive: isEditing && !hasExceededLicenseLimit,
                        message: "You can't proceed if you have unsaved nodes. Save your changes first.",
                    },
                ]}
            >
                <Button
                    disabled={isContinueDisabled}
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                >
                    Continue <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </ConditionalPopover>
        </div>
    );
}

function useRevalidatePersistedNodesOnEntry() {
    const { setValue, getValues } = useFormContext<SetupWizardFormData>();

    useEffect(() => {
        const nodes = getValues("nodeAddressStep.nodes");
        const securityOption = getValues("securityStep.securityOption");
        const cns = getValues("selfSignedCertificateStep.cns");

        nodes.forEach((node, index) => {
            if (securityOption !== "ownCertificate" && node.dnsName) {
                setValue(`nodeAddressStep.nodes.${index}.dnsName`, null);
            }

            if (securityOption === "ownCertificate" && node.dnsName) {
                const isDnsNameInCns = cns?.some((cn) => cn === node.dnsName);
                if (!isDnsNameInCns) {
                    if (cns.length === 1) {
                        // set first CN if only one is available
                        setValue(`nodeAddressStep.nodes.${index}.dnsName`, cns[0]);
                    } else {
                        setValue(`nodeAddressStep.nodes.${index}.dnsName`, null);
                    }
                }
            }

            /*
             * “Discard” restores the node to its pre-edit state. If the user returned here after changing earlier steps, restoring is not allowed because it would cause a validation error, the node must be either saved or removed.
             * When `isNewlyAdded` is true, the “Remove” button is shown instead of “Discard”.
             */
            setValue(`nodeAddressStep.nodes.${index}.isNewlyAdded`, true, {
                shouldValidate: true,
            });
            setValue(`nodeAddressStep.nodes.${index}.isEditing`, true, {
                shouldValidate: true,
            });
        });
    }, []); // eslint-disable-line react-hooks/exhaustive-deps
}

function LicenseLimitExceededMessage() {
    const { getValues } = useFormContext<SetupWizardFormData>();

    const nodeData = getValues("nodeAddressStep.nodes");
    const licenseKeyStep = getValues("licenseKeyStep");

    const maxClusterSize = licenseKeyStep?.licenseInfo?.maxClusterSize ?? setupWizardConstants.AGPL_MAX_CLUSTER_SIZE;
    const currentNodeCount = nodeData?.length;

    return (
        <>
            <p className="mb-0">
                Your license allows maximum <strong>{maxClusterSize}</strong> node(s), but you have configured{" "}
                <strong>{currentNodeCount}</strong> nodes.
            </p>
            <p className="mb-0">
                Remove <strong>{currentNodeCount - maxClusterSize}</strong> node(s) or upgrade your license to continue.
            </p>
            <hr className="my-2" />
            <span className="md-label">
                <Icon icon="link" /> See{" "}
                <a href="https://ravendb.net/buy" target="_blank">
                    licenses comparison <Icon icon="newtab" />
                </a>
            </span>
        </>
    );
}

export const ipAddressFormSchema = yup.object().shape({
    ipAddress: yup
        .string()
        .test(
            "not-url",
            "Expected valid IP Address/Hostname, not URL",
            (value) => !value?.startsWith("http://") && !value?.startsWith("https://")
        )
        .test(
            "not-localhost-on-docker",
            "A localhost IP Address is not allowed when running on Docker",
            function (value) {
                const { isDocker } = this.options.context || {};
                return (isDocker && !genUtils.isLocalhostIpAddress(value)) || !isDocker;
            }
        )
        .test("valid-ip-in-unsecure-mode", "In unsecure mode you cannot use hostnames", function (value) {
            const context = this.options.context as {
                nodeAddressStep: SetupWizardFormData["nodeAddressStep"];
                securityOption: SetupWizardSecurityOption;
                currentIndex: number;
            };

            const isHostname = genUtils.isHostname(value);
            const isUnsecureMode = context?.securityOption === "none";

            return !isHostname || !isUnsecureMode;
        })
        .required("Please define at least one IP for this node"),
});

export const nodeEditFormSchema = yup.object({
    isPassive: yup.boolean().default(false),
    nodeTag: yup.string().when("isPassive", {
        is: false,
        then: (schema) =>
            schema
                .required("Node tag is required")
                .matches(/^[A-Z]{1,4}$/, "Node tag must be 1 to 4 uppercase letters")
                .test("unique", "Node tag must be unique", function (value) {
                    const { nodeAddressStep, currentIndex } = this.options.context as {
                        nodeAddressStep: SetupWizardFormData["nodeAddressStep"];
                        setupWizardFormData: SetupWizardFormData;
                        currentIndex: number;
                    };

                    if (!nodeAddressStep || !nodeAddressStep.nodes) {
                        return true;
                    }

                    return (
                        nodeAddressStep.nodes.findIndex(
                            (node, idx) => node.nodeTag === value && idx !== currentIndex
                        ) === -1
                    );
                })
                .test("reserved-tag", "This node tag is reserved", (value) => {
                    return value !== "RAFT";
                }),
        otherwise: (schema) => schema,
    }),
    dnsName: yup
        .string()
        .nullable()
        .test("required-for-non-wildcard", "DNS name is required", function (value) {
            const { securityOption, isWildcardCertificate } = this.options.context as {
                securityOption: SetupWizardSecurityOption;
                isWildcardCertificate: boolean;
                cns: string[];
            };

            if (securityOption === "ownCertificate" && !isWildcardCertificate) {
                return !!value;
            }

            return true;
        })
        .test("valid-cn", "DNS name must be from the certificate", function (value) {
            const { securityOption, isWildcardCertificate, cns } = this.options.context as {
                securityOption: SetupWizardSecurityOption;
                isWildcardCertificate: boolean;
                cns: string[];
            };

            if (securityOption === "ownCertificate" && !isWildcardCertificate && value) {
                return cns.includes(value);
            }

            return true;
        }),
    httpPort: yup
        .number()
        .nullable()
        .typeError("HTTPS port must be a number")
        .min(1, "Port must be greater than 0")
        .max(65535, "Port must be less than 65536"), // 'required' validation is handled separately when saving
    tcpPort: yup
        .number()
        .default(38888)
        .nullable()
        .min(1, "Port must be greater than 0")
        .max(65535, "Port must be less than 65536")
        .required("TCP port is required"),
    ipAddress: yup.array().of(ipAddressFormSchema).min(1, "At least one IP address is required"),
    hasExternalConfig: yup.boolean().default(false),
    externalIpAddress: yup
        .string()
        .nullable()
        .when(["hasExternalConfig", "ipAddress", "$securityOption"], {
            is: function (
                hasExtConfig: boolean,
                ipAddresses: NodeEditFormData["ipAddress"],
                securityOption: SetupWizardSecurityOption
            ) {
                if (securityOption !== "letsEncrypt") {
                    return false;
                }

                const hasZeroAddress = ipAddresses?.some((ip) => ip?.ipAddress === "0.0.0.0");

                return hasExtConfig || hasZeroAddress;
            },
            then: (schema) =>
                schema
                    .required(
                        "External IP address is required, it tells RavenDB how to identify itself to other nodes and clients"
                    )
                    .test(
                        "not-url",
                        "Expected valid IP address, not URL",
                        (value) => !value?.startsWith("http://") && !value?.startsWith("https://")
                    )
                    .test(
                        "valid-ipv4",
                        "Please enter a valid IPv4 address (hostname not allowed, no port)",
                        (value) => !!value && genUtils.regexIPv4.test(value)
                    ),
            otherwise: (schema) =>
                schema.test(
                    "valid-ipv4-optional",
                    "Please enter a valid IPv4 address (hostname not allowed, no port)",
                    (value) => {
                        if (value == null || value === "") {
                            return true;
                        }
                        return genUtils.regexIPv4.test(value);
                    }
                ),
        }),

    externalHttpPort: yup
        .number()
        .nullable()
        .when("hasExternalConfig", {
            is: true,
            then: (schema) => schema.min(1, "Port must be greater than 0").max(65535, "Port must be less than 65536"),
        }),
    externalTcpPort: yup
        .number()
        .nullable()
        .when("hasExternalConfig", {
            is: true,
            then: (schema) => schema.min(1, "Port must be greater than 0").max(65535, "Port must be less than 65536"),
        }),
});

export type NodeEditFormData = yup.InferType<typeof nodeEditFormSchema>;
