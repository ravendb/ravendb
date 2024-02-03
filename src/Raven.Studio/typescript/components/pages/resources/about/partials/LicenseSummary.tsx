import { Button, Card, CardBody, Col, Row, UncontrolledTooltip } from "reactstrap";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import { ConnectivityStatus, OverallInfoItem } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import LicenseType = Raven.Server.Commercial.LicenseType;
import registration from "viewmodels/shell/registration";
import { AsyncState } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "hooks/useServices";
import { useAccessManager } from "hooks/useAccessManager";
import forceLicenseUpdateCommand from "commands/licensing/forceLicenseUpdateCommand";
import licenseModel from "models/auth/licenseModel";
import useConfirm from "components/common/ConfirmDialog";

interface LicenseSummaryProps {
    asyncCheckLicenseServerConnectivity: AsyncState<ConnectivityStatus>;
    asyncGetConfigurationSettings: AsyncState<Raven.Server.Config.Categories.LicenseConfiguration>;
    recheckConnectivity: () => Promise<void>;
}

function canRenewLicense(licenseType: LicenseType) {
    return licenseType === "Developer" || licenseType === "Community";
}

export function LicenseSummary(props: LicenseSummaryProps) {
    const { recheckConnectivity, asyncCheckLicenseServerConnectivity, asyncGetConfigurationSettings } = props;

    const licenseStatus = useAppSelector(licenseSelectors.status);
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));
    const isIsv = useAppSelector(licenseSelectors.statusValue("IsIsv"));
    const expiration = licenseModel.formattedExpirationProvider(licenseStatus);

    const [refreshing, setRefreshing] = useState<boolean>(false);

    const refreshConnectivity = async () => {
        setRefreshing(true);
        try {
            await recheckConnectivity();
        } finally {
            setRefreshing(false);
        }
    };

    return (
        <Card>
            <CardBody>
                <h4>License</h4>
                <Row>
                    <OverallInfoItem icon="license" label="License type">
                        <span className={classNames({ "text-cloud": isCloud })}>
                            {licenseModel.licenseTypeTextProvider(licenseStatus)}
                        </span>
                    </OverallInfoItem>
                    {expiration && (
                        <OverallInfoItem icon="calendar" label={isIsv ? "Updates Expiration" : "Expires"}>
                            {expiration.formattedDate} <br />
                            <small className={expiration.timeClass}>{expiration.relativeTime}</small>
                            {isCloud && (
                                <div>
                                    <small>Cloud licenses are automatically renewed</small>
                                </div>
                            )}
                        </OverallInfoItem>
                    )}

                    <OverallInfoItem icon="raven" label="License server">
                        <ConnectivityStatusComponent
                            refreshing={refreshing}
                            refresh={refreshConnectivity}
                            status={asyncCheckLicenseServerConnectivity}
                        />
                    </OverallInfoItem>
                    <LicenseActions asyncGetConfigurationSettings={asyncGetConfigurationSettings} />
                </Row>
            </CardBody>
        </Card>
    );
}

function ConnectivityStatusComponent(props: {
    status: AsyncState<ConnectivityStatus>;
    refreshing: boolean;
    refresh: () => void;
}) {
    const { status, refresh, refreshing } = props;

    if (status.loading) {
        return (
            <LazyLoad active>
                <div>Loading placeholder</div>
            </LazyLoad>
        );
    }

    if (status.status === "error") {
        return <LoadError />;
    }

    if (status.result.connected) {
        return (
            <span className="text-success">
                <Icon icon="check" />
                Connected
            </span>
        );
    }

    return (
        <span className="text-warning" id="connectivityException">
            <Icon icon="warning" />
            <small>
                Unable to reach the RavenDB License Server at <code>api.ravendb.net</code>
            </small>
            <ButtonWithSpinner isSpinning={refreshing} outline className="ms-2 rounded-pill" onClick={refresh}>
                <Icon icon="refresh" title="Click to check connection" /> Test again
            </ButtonWithSpinner>
            <UncontrolledTooltip target="connectivityException">
                Exception: {status.result.exception}
            </UncontrolledTooltip>
        </span>
    );
}

interface LicenseActionsProps {
    asyncGetConfigurationSettings: AsyncState<Raven.Server.Config.Categories.LicenseConfiguration>;
}

function LicenseActions(props: LicenseActionsProps) {
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const licenseRegistered = useAppSelector(licenseSelectors.licenseRegistered);

    const confirm = useConfirm();
    const { licenseService } = useServices();
    const { isClusterAdminOrClusterNode } = useAccessManager();
    const [forcingUpdate, setForcingUpdate] = useState<boolean>(false);

    const { asyncGetConfigurationSettings } = props;

    if (asyncGetConfigurationSettings.status !== "success") {
        return null;
    }

    const registerLicense = () => registration.showRegistrationDialog(licenseStatus, false, true);
    const renewLicense = () => registration.showRegistrationDialog(licenseStatus, false, true, true);

    const licenseConfiguration = asyncGetConfigurationSettings.result;

    if (licenseRegistered) {
        const isReplaceLicenseEnabled = licenseConfiguration.CanActivate && isClusterAdminOrClusterNode();
        const isForceUpdateEnabled = licenseConfiguration.CanForceUpdate && isClusterAdminOrClusterNode();
        const isRenewLicenseEnabled = licenseConfiguration.CanRenew && isClusterAdminOrClusterNode();

        const forceUpdate = async () => {
            const confirmed = await confirm({
                title: "Force License Update",
                message: "Are you sure that you want to force license update?",
            });

            if (!confirmed) {
                return;
            }

            setForcingUpdate(true);
            try {
                const updateResult = await licenseService.forceUpdate();
                await licenseModel.fetchLicenseStatus();

                if (updateResult.Status === "NotModified") {
                    forceLicenseUpdateCommand.handleNotModifiedStatus(licenseStatus.Expired);
                }
                await licenseModel.fetchSupportCoverage();
            } finally {
                setForcingUpdate(false);
            }
        };

        return (
            <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
                {canRenewLicense(licenseStatus.Type) && (
                    <React.Fragment key="renew-container">
                        <span id="renew-license-btn">
                            <Button
                                outline
                                className="rounded-pill"
                                onClick={renewLicense}
                                disabled={!isRenewLicenseEnabled}
                            >
                                <Icon icon="reset" /> Renew license
                            </Button>
                        </span>

                        <LicenseTooltip
                            target="renew-license-btn"
                            operationEnabledInConfiguration={licenseConfiguration.CanRenew}
                            hasPrivileges={isClusterAdminOrClusterNode()}
                            operationAction="renew license. Expiration date will be extended"
                            operationTitle="Renew"
                        />
                    </React.Fragment>
                )}
                <span id="replace-license-btn">
                    <Button
                        outline
                        className="rounded-pill"
                        onClick={registerLicense}
                        disabled={!isReplaceLicenseEnabled}
                    >
                        <Icon icon="replace" /> Replace
                    </Button>
                </span>

                <span id="force-update-license-btn">
                    <ButtonWithSpinner
                        isSpinning={forcingUpdate}
                        outline
                        disabled={!isForceUpdateEnabled}
                        className="rounded-pill"
                        onClick={forceUpdate}
                    >
                        <Icon icon="force" /> Force Update
                        <Icon icon="info" color="info" margin="ms-1" />
                    </ButtonWithSpinner>
                </span>

                <LicenseTooltip
                    target="replace-license-btn"
                    operationEnabledInConfiguration={licenseConfiguration.CanActivate}
                    hasPrivileges={isClusterAdminOrClusterNode()}
                    operationAction="replace the current license with another license"
                    operationTitle="Replacing license"
                />
                <LicenseTooltip
                    target="force-update-license-btn"
                    operationEnabledInConfiguration={licenseConfiguration.CanForceUpdate}
                    hasPrivileges={isClusterAdminOrClusterNode()}
                    operationAction="apply the license that was set for you"
                    operationTitle="Force license update"
                />
            </Col>
        );
    }

    const isRegisterLicenseEnabled = licenseConfiguration.CanActivate && isClusterAdminOrClusterNode();

    return (
        <Col className="d-flex flex-wrap gap-2 align-items-center justify-content-end">
            <Button
                color="primary"
                className="rounded-pill"
                onClick={registerLicense}
                disabled={!isRegisterLicenseEnabled}
                id="replace-license-btn"
            >
                <Icon icon="replace" /> Register license
            </Button>
            <LicenseTooltip
                target="replace-license-btn"
                operationEnabledInConfiguration={licenseConfiguration.CanActivate}
                hasPrivileges={isClusterAdminOrClusterNode()}
                operationAction="register a new license"
                operationTitle="Registering new license"
            />
        </Col>
    );
}

function LicenseTooltip(props: {
    target: string;
    operationEnabledInConfiguration: boolean;
    hasPrivileges: boolean;
    operationAction: string;
    operationTitle: string;
}) {
    const { target, operationEnabledInConfiguration, operationTitle, operationAction, hasPrivileges } = props;

    let msg = operationEnabledInConfiguration && hasPrivileges ? `Click to ${operationAction}` : "";

    if (!operationEnabledInConfiguration) {
        msg = `${operationTitle} is disabled in the server configuration.`;
    }

    if (!hasPrivileges) {
        msg += " You have insufficient privileges. Only a Cluster Admin can do this.";
    }

    if (!msg) {
        return null;
    }

    return <UncontrolledTooltip target={target}>{msg}</UncontrolledTooltip>;
}
