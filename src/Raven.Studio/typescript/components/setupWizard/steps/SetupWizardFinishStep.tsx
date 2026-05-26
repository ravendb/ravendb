import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Switch } from "components/common/Checkbox";
import { FormGroup } from "components/common/Form";
import useBoolean from "components/hooks/useBoolean";
import { useEffect, useMemo, useState } from "react";
import { useServices } from "components/hooks/useServices";
import serverNotificationCenterClient from "common/serverNotificationCenterClient";
import { ThemeColor } from "components/models/common";
import endpoints from "endpoints";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import genUtils from "common/generalUtils";
import Tab from "react-bootstrap/Tab";
import Nav from "react-bootstrap/Nav";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import RichAlert from "components/common/RichAlert";
import { NumberedList, NumberedListItem } from "components/common/NumberedList";
import Modal from "components/common/Modal";
import Spinner from "react-bootstrap/Spinner";
import { useRavenLink } from "hooks/useRavenLink";
import classNames from "classnames";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import Code from "components/common/Code";
import { useAppDispatch } from "components/store";
import { setupWizardActions } from "components/setupWizard/store/setupWizardSlice";
import OperationStatus = Raven.Client.Documents.Operations.OperationStatus;

interface Logs {
    message: string;
    color?: ThemeColor;
}

export function SetupWizardFinishStep() {
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();
    const dispatch = useAppDispatch();

    const { value: isShowLogs, toggle: toggleIsShowLogs } = useBoolean(false);

    const { setupMethodStep, usePackageStep, finishStep } = useWatch({ control });

    const websocket = useMemo(() => new serverNotificationCenterClient(), []);

    const { databasesService, setupWizardService } = useServices();

    const [logs, setLogs] = useState<Logs[]>([]);
    const [errorLogs, setErrorLogs] = useState<Logs[]>([]);
    const [configurationProcess, setConfigurationProcess] =
        useState<Raven.Server.Commercial.SetupProgressAndResult>(null);

    const handleSetFinishStatus = (status: OperationStatus) => {
        dispatch(setupWizardActions.finishStepStatusSet(status));
        setValue("finishStep.finishingStatus", status);
        reportEvent(setupWizardGA4Prefixes.finalStep, "status", status);
    };

    const handleWebSocketOperation = (operation: Raven.Server.NotificationCenter.Notifications.OperationChanged) => {
        if (operation.TaskType === "Setup") {
            let dto: Raven.Server.Commercial.SetupProgressAndResult = operation.State.Progress;

            switch (operation.State.Status) {
                case "Completed":
                    dto = operation.State.Result as Raven.Server.Commercial.SetupProgressAndResult;
                    setConfigurationProcess(operation.State.Result);
                    handleSetFinishStatus("Completed");
                    break;
                case "InProgress":
                    dto = operation.State.Progress as Raven.Server.Commercial.SetupProgressAndResult;
                    setConfigurationProcess(operation.State.Progress);
                    handleSetFinishStatus("InProgress");
                    break;
                case "Faulted": {
                    setConfigurationProcess(operation.State.Progress);
                    const failure = operation.State
                        .Result as Raven.Client.Documents.Operations.OperationExceptionResult;

                    setLogs((prev) => [...prev, { message: failure.Message, color: "danger" }]);
                    setLogs((prev) => [...prev, { message: operation.State.Result.Error, color: "danger" }]);
                    setErrorLogs((prev) => [...prev, { message: failure.Error, color: "danger" }]);

                    handleSetFinishStatus("Faulted");
                    break;
                }
                case "Canceled":
                    dto = operation.State.Result as Raven.Server.Commercial.SetupProgressAndResult;
                    setConfigurationProcess(operation.State.Result);
                    handleSetFinishStatus("Canceled");
                    break;
            }

            if (dto) {
                switch (operation.TaskType) {
                    case "Setup":
                        setLogs(dto.Messages.map((message) => ({ message })));
                        break;
                }
            }
        }
    };

    const { getRegularDto, getContinueWithPackageDto, getSubmitUrlBase, downloadConfigurationLog } =
        useSetupWizardFinishUtils();

    const regularFinish = async () => {
        const operationId = await databasesService.getNextOperationId(null);
        const operationPart = "?operationId=" + operationId;
        const urlBase = getSubmitUrlBase();

        const dto = getRegularDto();

        const form = document.getElementById("setupForm") as HTMLFormElement;
        const optionsInput = form.querySelector("[name=Options]") as HTMLInputElement;

        form.action = urlBase + operationPart;
        optionsInput.value = JSON.stringify(dto);
        form.submit();

        websocket.watchOperation(operationId, handleWebSocketOperation);
    };

    const continueWithPackageFinish = async () => {
        const operationId = await databasesService.getNextOperationId(null);

        websocket.watchOperation(operationId, handleWebSocketOperation);

        const dto = getContinueWithPackageDto();

        if (usePackageStep.isZipSecure) {
            await setupWizardService.continueSecureClusterConfiguration(operationId, dto);
        } else {
            await setupWizardService.continueUnsecureClusterConfiguration(operationId, dto);
        }
    };

    useEffect(() => {
        const finish = async () => {
            reportEvent(
                setupWizardGA4Prefixes.finalStep,
                "start-setup",
                setupMethodStep.method === "usePackage" ? "usePackage" : "regular"
            );
            if (setupMethodStep.method === "usePackage") {
                await continueWithPackageFinish();
            } else {
                await regularFinish();
            }
        };

        finish();
    }, []);

    return (
        <div className="finish-step">
            <TopInfo />
            <div
                className={classNames(
                    "hstack mt-4 mb-1",
                    finishStep.finishingStatus === "Completed" ? "justify-content-end" : "justify-content-between"
                )}
            >
                {finishStep.finishingStatus !== "Completed" && (
                    <FormGroup marginClass="mb-0">
                        <Switch
                            className="mb-0"
                            selected={isShowLogs}
                            toggleSelection={() => {
                                reportEvent(
                                    setupWizardGA4Prefixes.finalStep,
                                    "toggle-logs",
                                    isShowLogs ? "hidden" : "shown"
                                );
                                toggleIsShowLogs();
                            }}
                            color="primary"
                        >
                            Show configuration log
                        </Switch>
                    </FormGroup>
                )}
                <Button
                    disabled={!configurationProcess?.Messages?.length && finishStep.finishingStatus !== "InProgress"}
                    variant="link"
                    onClick={() => {
                        reportEvent(setupWizardGA4Prefixes.finalStep, "download-log");
                        downloadConfigurationLog(
                            configurationProcess?.Messages ?? [],
                            `configurationLog_${moment.utc().format("YYYY-MM-DD-HH-mm-ss")}`
                        );
                    }}
                    size="xs"
                >
                    <Icon icon="download" />
                    Download configuration log
                </Button>
            </div>
            {finishStep.finishingStatus !== "Completed" && (
                <>
                    {!isShowLogs && configurationProcess && (
                        <div className="summary-tab-container mb-4">
                            <pre className="p-4 mb-0">
                                <Configuration configurationProcess={configurationProcess} errorLogs={errorLogs} />
                            </pre>
                        </div>
                    )}
                    {isShowLogs && (
                        <>
                            <div className="mb-4">
                                <Code
                                    className="border rounded"
                                    code={logs
                                        .map((message) =>
                                            message.color
                                                ? `<span class="text-${message.color}">${message.message}</span>`
                                                : message.message
                                        )
                                        .join("\n")}
                                    language="plaintext"
                                />
                            </div>
                        </>
                    )}
                </>
            )}
            {finishStep.finishingStatus === "Completed" && <CompletedSummary />}

            <div className="d-none">
                <form method="post" target="hidden-form" id="setupForm">
                    <input type="hidden" name="Options" />
                </form>
            </div>
        </div>
    );
}

interface ConfigurationProps {
    configurationProcess: Raven.Server.Commercial.SetupProgressAndResult;
    errorLogs: Logs[];
}

const Configuration = ({ configurationProcess, errorLogs }: ConfigurationProps) => {
    return (
        <div>
            <ConfigurationItem
                stepTitle="Validation"
                configurationState={configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.Validation}
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="Let's encrypt"
                configurationState={configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.LetsEncrypt}
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="DNS records"
                configurationState={configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.DnsRecords}
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="Acquiring let's encrypt certificate"
                configurationState={
                    configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.AcquiringLetsEncryptCertificate
                }
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="Configuration settings"
                configurationState={
                    configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.ConfigurationSettings
                }
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="Client certificate"
                configurationState={
                    configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.ClientCertificate
                }
                errorLogs={errorLogs}
            />

            <ConfigurationItem
                stepTitle="Creating settings.json"
                configurationState={
                    configurationProcess?.SetupActionSteps?.StepsByConfigurationStepType.CreatingSettingsJson
                }
                errorLogs={errorLogs}
            />
        </div>
    );
};

interface ConfigurationItemProps {
    stepTitle: string;
    configurationState: Raven.Server.Commercial.SetupActionInfo;
    errorLogs: Logs[];
}

const ConfigurationItem = ({ configurationState, stepTitle, errorLogs }: ConfigurationItemProps) => {
    if (!configurationState?.State || configurationState?.State === "NotApplicable") {
        return null;
    }

    const getConfigurationItemStatus = () => {
        switch (configurationState?.State) {
            case "Pending":
                return null;
            case "InProgress":
                return <Spinner className="spinner-gradient" size="sm" />;
            case "Completed":
                return <Icon color="success" icon="checkmark" margin="m-0" />;
            case "SkippedDueToError":
                return <Icon color="muted" icon="skip" margin="m-0" />;
            default:
                return <Icon color="danger" icon="close" margin="m-0" />;
        }
    };

    return (
        <div className="d-flex flex-column mb-1 align-items-center">
            <div className="w-100 d-flex align-items-center justify-content-between">
                <span className={classNames({ "opacity-25": configurationState?.State === "Pending" })}>
                    {stepTitle}
                </span>
                {getConfigurationItemStatus()}
            </div>
            {configurationState?.State === "Error" && (
                <RichAlert
                    style={{ height: "350px" }}
                    childrenClassName="w-100 overflow-auto h-100 d-flex align-items-stretch flex-column text-truncate"
                    variant="danger"
                    title={configurationState?.ErrorType}
                    className="my-2"
                    copyText={errorLogs.map((log) => log.message).join("\n")}
                    copyTextSuccessMessage="Error logs copied to clipboard"
                >
                    <div className="text-wrap" title={configurationState?.ErrorMessage}>
                        {configurationState?.ErrorMessage}
                    </div>
                    <div className="text-wrap">
                        {errorLogs.map((log, index) => (
                            <span key={index} className={classNames(log.color && `text-${log.color}`)}>
                                {log.message}
                            </span>
                        ))}
                    </div>
                </RichAlert>
            )}
        </div>
    );
};

function TopInfo() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        setupMethodStep,
        finishStep: { finishingStatus },
    } = useWatch({ control });

    if (finishingStatus === "InProgress") {
        return (
            <>
                <h2 className="mb-1">Configuration in progress</h2>
                <p className="mb-4 text-muted">
                    Please wait a moment.{" "}
                    {setupMethodStep.method === "createPackage"
                        ? "Your setup package will be ready shortly."
                        : "Your RavenDB server will be ready shortly."}
                </p>
            </>
        );
    }

    if (finishingStatus === "Faulted") {
        return (
            <>
                <h2 className="mb-1">Setup failed</h2>
                <p className="mb-4 text-muted">
                    It seems something went wrong. Refer to the error message for more details.
                </p>
            </>
        );
    }

    if (finishingStatus === "Canceled") {
        return (
            <>
                <h2 className="mb-1">Setup canceled</h2>
            </>
        );
    }

    if (finishingStatus === "Completed") {
        return (
            <>
                <h2 className="mb-1">All set!</h2>
                <p className="mb-4 text-muted">
                    You&apos;re almost ready to go. Follow the instructions to complete the process.
                </p>
            </>
        );
    }

    return null;
}

function CompletedSummary() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        nodeAddressStep,
        securityStep: { securityOption },
        usePackageStep: { nodeTag, isZipSecure },
        setupMethodStep: { method },
    } = useWatch({ control });

    const { getStudioUrl } = useSetupWizardFinishUtils();

    const isSettingCluster = nodeAddressStep.nodes.length > 1 || method === "usePackage";
    const localNode = nodeAddressStep.nodes?.[0];
    const localNodeTag = localNode?.nodeTag ?? nodeTag;
    const isSetupUnsecured = securityOption === "none" || (method === "usePackage" && !isZipSecure);
    const isSetupPackage = method === "createPackage";
    const showInfoAboutInstalledCertificate = !isSetupUnsecured && !isSetupPackage;
    const studioUrl = getStudioUrl();

    if (isSetupPackage) {
        return (
            <div className="summary-tab-container mb-6">
                <Nav className="mb-2">
                    <Nav.Item className="flex-grow">
                        <Nav.Link eventKey="cluster" className="cluster-tab">
                            Setting up a cluster
                        </Nav.Link>
                    </Nav.Item>
                </Nav>
                <div className="p-4">
                    <Row>
                        <Col
                            md={6}
                            className="vstack gap-3 text-center border-end px-4 border-secondary justify-content-center"
                        >
                            <div>
                                <Icon icon="folder" addon="attachment" color="primary" size="lg" />
                            </div>
                            <span>
                                Your cluster settings configuration and the certificate are included in the downloaded
                                zip file.
                            </span>
                        </Col>
                        <Col md={6} className="vstack gap-3 text-center justify-content-center px-4">
                            <Icon icon="cluster" color="node" size="lg" />
                            <span>
                                You are setting up a cluster. The cluster topology and node addresses have already been
                                configured.
                            </span>
                        </Col>
                    </Row>
                    <hr />
                    <h5>How to set up the cluster nodes?</h5>
                    <NumberedList>
                        <NumberedListItem stepKey={1}>
                            The next step is to download a new RavenDB server for each of the cluster nodes.
                        </NumberedListItem>
                        <NumberedListItem stepKey={2}>
                            When you enter the Setup Wizard on a new node, choose &apos;
                            <b>Use Setup Package</b>&apos;.
                            <br />
                            Do not start a setup process on a node that has already been configured; this is not
                            supported.
                        </NumberedListItem>
                        <NumberedListItem stepKey={3}>
                            You will be asked to upload the zip file that was just downloaded.
                        </NumberedListItem>
                        <NumberedListItem stepKey={4}>
                            The new server node will join the existing cluster.
                        </NumberedListItem>
                    </NumberedList>
                    <RichAlert variant="info" className="mt-3">
                        When the Setup Wizard is done and the new node restarts, the cluster will automatically detect
                        it.
                        <br />
                        There is no need to add it manually from Studio.
                        <br />
                        Simply access the &apos;Cluster&apos; view and observe the topology update.
                    </RichAlert>
                </div>
            </div>
        );
    }

    return (
        <div className="summary-tab-container mb-6">
            <Tab.Container id="summary-tabs" defaultActiveKey={isSettingCluster ? "cluster" : "connectToServer"}>
                <Nav className="mb-2">
                    <Nav.Item className="flex-grow">
                        <Nav.Link eventKey="connectToServer" className="connect-to-server-tab">
                            Access the server
                        </Nav.Link>
                    </Nav.Item>
                    {isSettingCluster && (
                        <Nav.Item className="flex-grow">
                            <Nav.Link eventKey="cluster" className="cluster-tab">
                                Complete cluster setup
                            </Nav.Link>
                        </Nav.Item>
                    )}
                </Nav>
                <Tab.Content className="p-4 text-break">
                    <Tab.Pane eventKey="connectToServer">
                        <Row>
                            <Col
                                md={showInfoAboutInstalledCertificate ? 4 : 6}
                                className="border-secondary border-end vstack gap-2 text-center justify-content-center"
                            >
                                <Icon icon="server" color="primary" size="lg" />
                                <span>
                                    The new server will be available at:{" "}
                                    <a href={studioUrl} target="_blank" className="pe-none">
                                        {studioUrl}
                                    </a>
                                </span>
                            </Col>
                            <Col
                                md={showInfoAboutInstalledCertificate ? 4 : 6}
                                className={classNames(
                                    "border-secondary vstack gap-2 text-center justify-content-center",
                                    {
                                        "border-end": showInfoAboutInstalledCertificate,
                                    }
                                )}
                            >
                                <Icon icon="node" color="node" size="lg" />
                                <span>
                                    The current <span className="text-node fw-bold">Node {localNodeTag}</span> has
                                    already been configured and requires no further action on your part.
                                </span>
                            </Col>
                            {showInfoAboutInstalledCertificate && (
                                <Col md={4} className="vstack gap-2 text-center justify-content-center">
                                    <Icon icon="certificate" size="lg" />
                                    <span>An administrator client certificate has been installed on this machine.</span>
                                </Col>
                            )}
                        </Row>
                        <RichAlert variant="info" className="mt-3">
                            You&apos;ll need to restart the server before you can access RavenDB Studio.
                        </RichAlert>
                    </Tab.Pane>
                    <Tab.Pane eventKey="cluster">
                        <Row>
                            <Col md={6} className="vstack gap-2 text-center justify-content-center">
                                <div>
                                    <Icon icon="folder" addon="attachment" color="primary" size="lg" />
                                </div>
                                <span>The new server will be available at: {studioUrl}</span>
                            </Col>
                            <Col md={6} className="vstack gap-2 text-center justify-content-center">
                                <Icon icon="cluster" color="node" size="lg" />
                                <span>
                                    The current <span className="text-node fw-bold">Node {nodeTag}</span> has already
                                    been configured and requires no further action on your part.
                                </span>
                            </Col>
                        </Row>
                        <hr />
                        <h5>How to set up the other nodes?</h5>
                        <NumberedList>
                            <NumberedListItem stepKey={1}>
                                The next step is to download a new RavenDB server for each of the cluster nodes.
                            </NumberedListItem>
                            <NumberedListItem stepKey={2}>
                                When you enter the Setup Wizard on a new node, choose &apos;
                                <b>Use Setup Package</b>&apos;.
                                <br />
                                Do not start a setup process on a node that has already been configured; this is not
                                supported.
                            </NumberedListItem>
                            <NumberedListItem stepKey={3}>
                                You will be asked to upload the zip file that was just downloaded.
                            </NumberedListItem>
                            <NumberedListItem stepKey={4}>
                                The new server node will join the existing cluster.
                            </NumberedListItem>
                        </NumberedList>
                        <RichAlert variant="info" className="mt-2">
                            When the Setup Wizard is done and the new node restarts, the cluster will automatically
                            detect it.
                            <br />
                            There is no need to add it manually from Studio.
                            <br />
                            Simply access the &apos;Cluster&apos; view and observe the topology update.
                        </RichAlert>
                    </Tab.Pane>
                </Tab.Content>
            </Tab.Container>
        </div>
    );
}

function useSetupWizardFinishUtils() {
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        nodeAddressStep,
        securityStep,
        setupMethodStep,
        licenseKeyStep,
        domainStep,
        selfSignedCertificateStep,
        usePackageStep,
        additionalSettingsStep,
    } = useWatch({ control });

    const getLocalNode = () => {
        return nodeAddressStep.nodes[0];
    };

    const getNodeInfo = (
        node: SetupWizardFormData["nodeAddressStep"]["nodes"][number]
    ): Raven.Server.Commercial.NodeInfo => {
        return {
            Addresses: node.ipAddress.map((x) => x.ipAddress),
            Port: getHttpPort(node.httpPort),
            TcpPort: getTcpPort(node.tcpPort),
            PublicServerUrl: getServerUrl(node.dnsName, node.httpPort),
            PublicTcpServerUrl: null,
            ExternalIpAddress: node.hasExternalConfig ? node.externalIpAddress : null,
            ExternalPort: node.hasExternalConfig ? node.externalHttpPort : null,
            ExternalTcpPort: node.hasExternalConfig ? node.externalTcpPort : null,
        };
    };

    const getNodeSetupInfos = (): Record<string, Raven.Server.Commercial.NodeInfo> => {
        const nodesInfo: Record<string, Raven.Server.Commercial.NodeInfo> = {};
        nodeAddressStep.nodes.forEach((node) => {
            nodesInfo[node.nodeTag ?? "A"] = getNodeInfo(node);
        });

        return nodesInfo;
    };

    const getUnsecuredDto = (): Raven.Server.Commercial.UnsecuredSetupInfo => {
        const localNode = getLocalNode();

        return {
            AutoIndexingEngineType: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.autoIndexingEngineType
                : null,
            DataDirectory: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.dataDirectory
                : null,
            SetupCertificatePath: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.setupCertificatePath
                : null,
            LogsPath: additionalSettingsStep.isAdvancedSettingsVisible ? additionalSettingsStep.logsPath : null,
            StaticIndexingEngineType: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.staticIndexingEngineType
                : null,
            EnableExperimentalFeatures: additionalSettingsStep.postgresqlIntegration,
            LocalNodeTag: localNode.nodeTag,
            Environment: additionalSettingsStep.studioEnvironment,
            License: licenseKeyStep.key == "" ? null : JSON.parse(licenseKeyStep.key),
            ZipOnly: setupMethodStep.method === "createPackage",
            StartAsPassive: localNode.isPassive ?? false,
            NodeSetupInfos: getNodeSetupInfos(),
        };
    };

    const getSecuredDto = (): Raven.Server.Commercial.SetupInfo => {
        const { adminCertificateExpirationTime } = additionalSettingsStep;
        const localNode = getLocalNode();

        const ClientCertNotAfter = adminCertificateExpirationTime
            ? moment.utc().add(additionalSettingsStep.adminCertificateExpirationTime, "months").format()
            : null;

        return {
            AutoIndexingEngineType: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.autoIndexingEngineType
                : null,
            DataDirectory: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.dataDirectory
                : null,
            SetupCertificatePath: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.setupCertificatePath
                : null,
            LogsPath: additionalSettingsStep.isAdvancedSettingsVisible ? additionalSettingsStep.logsPath : null,
            StaticIndexingEngineType: additionalSettingsStep.isAdvancedSettingsVisible
                ? additionalSettingsStep.staticIndexingEngineType
                : null,
            EnableExperimentalFeatures: additionalSettingsStep.postgresqlIntegration,
            Environment: additionalSettingsStep.studioEnvironment,
            License: JSON.parse(licenseKeyStep.key),
            Email: domainStep.email,
            Domain: domainStep.domain,
            RootDomain: domainStep.rootDomain,
            LocalNodeTag: localNode.nodeTag,
            RegisterClientCert: true, // it should be always true. we should detect if same cert is installed and if no then install
            Certificate: selfSignedCertificateStep.certificate,
            Password: selfSignedCertificateStep.password,
            ClientCertNotAfter,
            ZipOnly: setupMethodStep.method === "createPackage",
            StartAsPassive: localNode.isPassive ?? false,
            NodeSetupInfos: getNodeSetupInfos(),
        };
    };

    const getRegularDto = () => {
        if (!securityStep.securityOption) {
            return null;
        }

        switch (securityStep.securityOption) {
            case "none":
                return getUnsecuredDto();
            case "letsEncrypt":
            case "ownCertificate":
                return getSecuredDto();
            default:
                assertUnreachable(securityStep.securityOption);
        }
    };

    const getContinueWithPackageDto = (): Raven.Server.Commercial.ContinueSetupInfo => {
        return {
            NodeTag: usePackageStep.nodeTag,
            Zip: usePackageStep.fileZip,
            RegisterClientCert: usePackageStep.isZipSecure, // we should detect if the same cert is installed and if no then install it
        };
    };

    const getHttpPort = (port: number) => {
        if (!port && securityStep.securityOption === "none") {
            return 8080;
        }
        return port;
    };

    const getTcpPort = (port: number) => {
        if (!port && securityStep.securityOption === "none") {
            return 38888;
        }
        return port;
    };

    const formatIpAddress = (ip: string): string => {
        const address = genUtils.getAddressInfo(ip);
        if (address.Type === "ipv6" && !ip.startsWith("[") && !ip.endsWith("]")) {
            return `[${ip}]`;
        }
        return ip;
    };

    const getPortPart = () => {
        const port = getLocalNode().httpPort;
        return port && port !== 443 ? ":" + port : "";
    };

    const getServerUrl = (dnsName: string, port: number) => {
        if (!dnsName) {
            return null;
        }

        let serverUrl = "https://" + dnsName;
        if (port && port !== 443) {
            serverUrl += ":" + port;
        }

        return serverUrl;
    };

    const getDomainForWildcard = (tag: string) => {
        if (selfSignedCertificateStep.cns.length === 0) {
            return "";
        }

        const cn = selfSignedCertificateStep.cns[0];

        if (!tag) {
            return cn.replace("*.", "");
        }
        return cn.replace("*", tag);
    };

    const getStudioUrl = () => {
        if (setupMethodStep.method === "usePackage") {
            return usePackageStep.publicServerUrl || usePackageStep.serverUrl;
        }

        if (securityStep.securityOption === "none") {
            const setupPort = getLocalNode().httpPort || 8080;
            const setupAddress = getLocalNode().ipAddress[0].ipAddress;

            let host;
            const port = setupPort;
            if (setupAddress === "0.0.0.0") {
                host = document.location.hostname;
            } else {
                host = formatIpAddress(setupAddress);
            }

            return `http://${host}:${port}`;
        }

        if (securityStep.securityOption === "letsEncrypt") {
            return `https://${getLocalNode().nodeTag.toLocaleLowerCase()}.${domainStep.domain}.${domainStep.rootDomain}${getPortPart()}`;
        }

        if (securityStep.securityOption === "ownCertificate") {
            const localNode = getLocalNode();

            if (selfSignedCertificateStep.isWildcardCertificate) {
                const domain = getDomainForWildcard(localNode.nodeTag.toLocaleLowerCase());
                return "https://" + domain + getPortPart();
            }

            return getServerUrl(localNode.dnsName, localNode.httpPort);
        }

        return null;
    };

    const getSubmitUrlBase = () => {
        if (!securityStep.securityOption) {
            return null;
        }

        switch (securityStep.securityOption) {
            case "none":
                return endpoints.global.setup.setupUnsecuredPackage;
            case "letsEncrypt":
                return endpoints.global.setup.setupLetsencrypt;
            case "ownCertificate":
                return endpoints.global.setup.setupSecured;
            default:
                assertUnreachable(securityStep.securityOption);
        }
    };

    const downloadConfigurationLog = (data: string[], fileName: string) => {
        if (!data || data.length === 0) {
            return;
        }

        const content = data.join("\n");

        const blob = new Blob([content], { type: "text/plain;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `${fileName}.txt`;
        a.click();

        URL.revokeObjectURL(url);
    };

    return {
        getStudioUrl,
        getHttpPort,
        getTcpPort,
        getServerUrl,
        getRegularDto,
        getContinueWithPackageDto,
        getSubmitUrlBase,
        downloadConfigurationLog,
    };
}

interface CertInstallationConfirmProps {
    onCancel: () => void;
    onConfirm: () => void;
}

function CertInstallationConfirm({ onCancel, onConfirm }: CertInstallationConfirmProps) {
    const { reportEvent } = useEventsCollector();

    const browser = genUtils.getBrowser();
    const [activeTab, setActiveTab] = useState(browser);
    const docsLink = useRavenLink({
        hash: "VD4R8E",
    });

    return (
        <Modal show onHide={onCancel} contentClassName="modal-border bulge-primary" size="lg">
            <Modal.Header closeButton onCloseClick={onCancel}>
                <Icon icon="certificate" color="primary" addon="check" className="fs-2" />
                <span className="lead">Confirm certificate installation</span>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <NumberedList>
                    <NumberedListItem stepKey={1}>
                        <h4>Recognize certificate in your browser</h4>
                        <div className="browser-tab-container">
                            <Tab.Container
                                id="summary-tabs"
                                activeKey={activeTab}
                                onSelect={(key: Browser) => setActiveTab(key)}
                            >
                                <Nav className="mb-2">
                                    <Nav.Item className="flex-grow">
                                        <Nav.Link eventKey="Chrome" className="chrome-tab">
                                            <Icon icon="chrome" />
                                            Chrome
                                        </Nav.Link>
                                    </Nav.Item>
                                    <Nav.Item className="flex-grow">
                                        <Nav.Link eventKey="Firefox" className="firefox-tab">
                                            <Icon icon="firefox" />
                                            Firefox
                                        </Nav.Link>
                                    </Nav.Item>
                                    <Nav.Item className="flex-grow">
                                        <Nav.Link eventKey="Safari" className="safari-tab">
                                            <Icon icon="safari" />
                                            Safari
                                        </Nav.Link>
                                    </Nav.Item>
                                    <Nav.Item className="flex-grow">
                                        <Nav.Link eventKey="Other" className="other-tab">
                                            <Icon icon="global" />
                                            Other
                                        </Nav.Link>
                                    </Nav.Item>
                                </Nav>
                                <Tab.Content className="p-2 text-break">
                                    <Tab.Pane eventKey="Chrome">
                                        Chrome (or any{" "}
                                        <a
                                            href="https://en.wikipedia.org/wiki/Chromium_(web_browser)#Browsers_based_on_Chromium"
                                            target="_blank"
                                        >
                                            Chromium-based browser
                                        </a>
                                        ) will let you select this certificate automatically. You may need to restart
                                        all instances of Chrome to make sure nothing is cached.
                                    </Tab.Pane>
                                    <Tab.Pane eventKey="Firefox">
                                        Firefox uses its own internal certificate store. After importing the certificate
                                        through Firefox settings, it will be available for use automatically. You may
                                        need to restart Firefox to ensure the new certificate is recognized properly.
                                    </Tab.Pane>
                                    <Tab.Pane eventKey="Safari">
                                        Safari uses the macOS Keychain to manage certificates. Once the certificate is
                                        imported and trusted in Keychain Access, Safari will select it automatically
                                        when needed. Restarting Safari or the system may help if it doesn’t appear right
                                        away.
                                    </Tab.Pane>
                                    <Tab.Pane eventKey="Other">
                                        Browsers that are not Chromium-based and don’t use the system certificate store
                                        typically require manual certificate import through their own settings or
                                        preferences. Behavior may vary, and restarting the browser is often recommended
                                        to ensure the certificate is applied.
                                    </Tab.Pane>
                                </Tab.Content>
                            </Tab.Container>
                        </div>
                    </NumberedListItem>
                    <NumberedListItem stepKey={2}>
                        <h4 className="mb-2">Restart server</h4>
                        <p className="mb-1">
                            Once you proceed with restart, pick your newly installed certificate from the list of
                            available certificates.
                        </p>
                        {activeTab === "Chrome" && (
                            <p className="mb-0">
                                If Chrome doesn&apos;t let you choose a certificate and instead you get a RavenDB
                                authentication error, please try again in the Incognito mode (or close all instances of
                                Chrome). It can happen because the browser caches the client certificates.
                            </p>
                        )}
                    </NumberedListItem>
                </NumberedList>
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <a
                    href={docsLink}
                    target="_blank"
                    className="btn btn-info rounded-pill"
                    onClick={() => reportEvent(setupWizardGA4Prefixes.finalStep, "open-docs", "cert-install")}
                >
                    See documentation <Icon icon="newtab" margin="m-0" />
                </a>
                <div className="hstack gap-2">
                    <Button
                        variant="link"
                        onClick={() => {
                            reportEvent(setupWizardGA4Prefixes.finalStep, "confirm-cert", "cancel");
                            onCancel();
                        }}
                        className="link-muted"
                    >
                        Cancel
                    </Button>
                    <Button
                        variant="primary"
                        onClick={() => {
                            reportEvent(setupWizardGA4Prefixes.finalStep, "confirm-cert", "confirm");
                            onConfirm();
                        }}
                        className="rounded-pill"
                    >
                        <Icon icon="reset" />
                        Restart server
                    </Button>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

export function SetupWizardFinishStepFooter() {
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();
    const { control, setValue, reset } = useFormContext<SetupWizardFormData>();
    const { securityStep, setupMethodStep, usePackageStep, finishStep } = useWatch({ control });
    const { value: isCertInstallationConfirmed, toggle: toggleIsCertInstallationConfirmed } = useBoolean(false);

    const { getStudioUrl } = useSetupWizardFinishUtils();

    const redirectToStudio = () => {
        reportEvent(setupWizardGA4Prefixes.finalStep, "redirect-to-studio");
        window.location.href = getStudioUrl();
    };

    const resetServer = async () => {
        reportEvent(setupWizardGA4Prefixes.finalStep, "restart-execute");
        await setupWizardService.finishSetup();

        const waitBeforeRedirectInMs = 2000;
        setTimeout(() => {
            redirectToStudio();
        }, waitBeforeRedirectInMs);
    };

    const handleRestart = useAsyncCallback(async () => {
        reportEvent(setupWizardGA4Prefixes.finalStep, "restart-click");
        const isSecure =
            (securityStep.securityOption !== null && securityStep.securityOption !== "none") ||
            (setupMethodStep.method === "usePackage" && usePackageStep.isZipSecure);

        if (isSecure) {
            reportEvent(setupWizardGA4Prefixes.finalStep, "open-cert-modal");
            toggleIsCertInstallationConfirmed();
        } else {
            reportEvent(setupWizardGA4Prefixes.finalStep, "restart-without-cert-modal");
            await resetServer();
        }
    });

    const finishStepIsDisabled =
        finishStep.finishingStatus === "InProgress" || finishStep.finishingStatus === "Faulted";

    const handleNewSetupPackage = () => {
        const defaultValues: SetupWizardFormData = {
            currentStep: "Setup method",
            setupMethodStep: {
                method: "usePackage",
            },
            usePackageStep: {
                fileZip: "",
                nodeTag: "",
                isZipValid: false,
                isZipSecure: false,
                publicServerUrl: "",
                serverUrl: "",
            },
            licenseKeyStep: {
                isAcceptTerms: false,
                isAcceptEmails: false,
                key: "",
                licenseInfo: null,
                licenseTypeToGenerate: null,
                firstName: "",
                lastName: "",
                email: "",
                phone: "",
            },
            domainStep: {
                domain: "",
                email: "",
            },
            securityStep: {
                securityOption: null,
            },
            selfSignedCertificateStep: {
                certificateFileName: "",
                certificate: "",
                password: "",
                cns: [],
            },
            nodeAddressStep: {
                nodes: [],
            },
            additionalSettingsStep: {
                isAdvancedSettingsVisible: false,
                dataDirectory: null,
                setupCertificatePath: null,
                adminCertificateExpirationTime: 60,
                postgresqlIntegration: false,
                studioEnvironment: "None",
            },
            finishStep: {
                finishingStatus: "InProgress" as const,
            },
        };

        reset(defaultValues);
    };

    return (
        <div
            className={classNames("d-flex justify-content-end", {
                "justify-content-between": finishStep.finishingStatus === "Faulted",
            })}
        >
            {finishStep.finishingStatus === "Faulted" && (
                <Button
                    variant="secondary"
                    className="rounded-pill mt-2"
                    onClick={() => setValue("currentStep", "Summary")}
                >
                    <Icon icon="arrow-left" />
                    Back
                </Button>
            )}
            {setupMethodStep.method === "createPackage" ? (
                <Button
                    disabled={finishStepIsDisabled}
                    variant="secondary"
                    className="mt-2 rounded-pill"
                    onClick={handleNewSetupPackage}
                >
                    Go to setup method
                </Button>
            ) : (
                <>
                    <ButtonWithSpinner
                        isSpinning={handleRestart.loading}
                        disabled={finishStepIsDisabled}
                        variant="primary"
                        onClick={handleRestart.execute}
                        className="mt-2 rounded-pill"
                        icon="reset"
                    >
                        Restart server
                    </ButtonWithSpinner>
                    {isCertInstallationConfirmed && (
                        <CertInstallationConfirm
                            onCancel={toggleIsCertInstallationConfirmed}
                            onConfirm={() => {
                                reportEvent(setupWizardGA4Prefixes.finalStep, "confirm-cert", "confirm");
                                resetServer();
                                toggleIsCertInstallationConfirmed();
                            }}
                        />
                    )}
                </>
            )}
        </div>
    );
}
