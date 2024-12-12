import React from "react";
import { Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { ServerSettingsVirtualTable } from "components/pages/resources/manageServer/serverSettings/ServerSettingsVirtualTable";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { ServerSettingsInfoHub } from "components/pages/resources/manageServer/serverSettings/ServerSettingsInfoHub";
import SizeGetter from "components/common/SizeGetter";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { settingsEntry } from "models/database/settings/databaseSettingsModels";
import { sortBy } from "common/typeUtils";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { ServerSettingsColumns } from "components/pages/resources/manageServer/serverSettings/useServerSettingsColumns";

export function ServerSettings() {
    const asyncFetchServerSettings = useFetchServerSettings();
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);

    if (!isClusterAdminOrClusterNode) {
        return (
            <div className="h-100 d-flex align-items-center justify-content-center">
                <FeatureNotAvailable badgeText="Insufficient access">
                    You are not authorized to view this page
                </FeatureNotAvailable>
            </div>
        );
    }

    return (
        <Row className="content-padding">
            <Col className="h-100" md={12} lg={7}>
                <div className="h-100 flex-column d-flex mb-4">
                    <AboutViewHeading title="Server Settings" icon="server-settings" />
                    {asyncFetchServerSettings.status !== "error" && (
                        <div className="d-flex justify-content-end">
                            <ButtonWithSpinner
                                className="justify-content-end"
                                onClick={asyncFetchServerSettings.execute}
                                isSpinning={asyncFetchServerSettings.loading}
                            >
                                <Icon icon="refresh" /> Refresh
                            </ButtonWithSpinner>
                        </div>
                    )}
                    <SizeGetter
                        render={(props) => (
                            <ServerSettingsVirtualTable
                                isLoading={asyncFetchServerSettings.loading}
                                data={asyncFetchServerSettings.result ?? []}
                                status={asyncFetchServerSettings.status}
                                reload={asyncFetchServerSettings.execute}
                                {...props}
                            />
                        )}
                    />
                </div>
            </Col>
            <Col md={12} lg={5}>
                <ServerSettingsInfoHub />
            </Col>
        </Row>
    );
}

function useFetchServerSettings() {
    const { manageServerService } = useServices();

    return useAsync(async () => {
        const serverSettings = await manageServerService.getServerSettings();
        const mappedSettings = serverSettings.Settings.map((setting) => settingsEntry.getEntry(setting));
        const sortedSettings = sortBy(mappedSettings, (setting) => setting.keyName());

        return sortedSettings.map(
            (setting): ServerSettingsColumns => ({
                configurationKey: setting.keyName(),
                configurationKeyTooltip: setting.descriptionText(),
                effectiveValue: setting.effectiveValue(),
                origin: setting.effectiveValueOrigin(),
            })
        );
    }, []);
}
