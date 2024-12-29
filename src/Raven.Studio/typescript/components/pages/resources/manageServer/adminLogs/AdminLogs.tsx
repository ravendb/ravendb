import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import Select, { SelectOption } from "components/common/select/Select";
import SizeGetter from "components/common/SizeGetter";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import AdminLogsVirtualList from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsVirtualList";
import AdminLogsDiskDownloadModal from "components/pages/resources/manageServer/adminLogs/disk/AdminLogsDiskDownloadModal";
import AdminLogsDiskSettingsModal from "components/pages/resources/manageServer/adminLogs/disk/settings/AdminLogsDiskSettingsModal";
import AdminLogsDisplaySettingsModal from "components/pages/resources/manageServer/adminLogs/displaySettings/AdminLogsDisplaySettingsModal";
import useAdminLogsFilter from "components/pages/resources/manageServer/adminLogs/hooks/useAdminLogsFilter";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import AdminLogsExportButton from "components/pages/resources/manageServer/adminLogs/view/AdminLogsExportButton";
import AdminLogsViewSettingsModal from "components/pages/resources/manageServer/adminLogs/view/AdminLogsViewSettingsModal";
import { useAppDispatch, useAppSelector } from "components/store";
import { logLevelOptions } from "components/utils/common";
import { useEffect } from "react";
import { StylesConfig } from "react-select";
import { Button, Card, CardBody, CardHeader, Input } from "reactstrap";

export default function AdminLogs() {
    const dispatch = useAppDispatch();
    const eventsCollector = useEventsCollector();
    const { manageServerService } = useServices();

    const isPaused = useAppSelector(adminLogsSelectors.isPaused);
    const isMonitorTail = useAppSelector(adminLogsSelectors.isMonitorTail);
    const isDiscSettingOpen = useAppSelector(adminLogsSelectors.isDiscSettingOpen);
    const isViewSettingOpen = useAppSelector(adminLogsSelectors.isViewSettingOpen);
    const isDisplaySettingsOpen = useAppSelector(adminLogsSelectors.isDisplaySettingsOpen);
    const isDownloadDiskLogsOpen = useAppSelector(adminLogsSelectors.isDownloadDiskLogsOpen);
    const isAllExpanded = useAppSelector(adminLogsSelectors.isAllExpanded);
    const configs = useAppSelector(adminLogsSelectors.configs);
    const configsLoadStatus = useAppSelector(adminLogsSelectors.configsLoadStatus);
    const filter = useAppSelector(adminLogsSelectors.filter);

    const { localFilter, handleFilterChange } = useAdminLogsFilter();

    // Fetch configs and start ws client on mount
    useEffect(() => {
        dispatch(adminLogsActions.fetchConfigs());
        dispatch(adminLogsActions.liveClientStarted());

        return () => {
            dispatch(adminLogsActions.liveClientStopped());
            dispatch(adminLogsActions.reset());
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const handlePageMinLevelChange = async ({ value }: SelectOption<Sparrow.Logging.LogLevel>) => {
        if (!configs) {
            return;
        }

        await manageServerService.saveAdminLogsConfiguration({
            AdminLogs: {
                MinLevel: value,
                Filters: configs.adminLogsConfig.AdminLogs.CurrentFilters,
                LogFilterDefaultAction: configs.adminLogsConfig.AdminLogs.CurrentLogFilterDefaultAction,
            },
        });
        dispatch(adminLogsActions.liveClientStopped());
        dispatch(adminLogsActions.liveClientStarted());
        await dispatch(adminLogsActions.fetchConfigs());
    };

    const enabledLogLevelOptions = logLevelOptions.filter((x) => x.value !== "Off");

    return (
        <div className="content-padding vstack gap-3 h-100">
            <div className="hstack flex-wrap gap-1">
                <div className="flex-grow-1">
                    <Card>
                        <CardHeader className="d-flex justify-content-between">
                            <h4 className="mb-0">
                                <Icon icon="client" />
                                Logs on this view
                            </h4>
                            <div className="d-flex align-items-center">
                                <Icon icon="logs" addon="arrow-filled-up" />
                                Min level:{" "}
                                <Select
                                    value={enabledLogLevelOptions.find(
                                        (x) => x.value === configs?.adminLogsConfig?.AdminLogs?.CurrentMinLevel
                                    )}
                                    onChange={handlePageMinLevelChange}
                                    options={enabledLogLevelOptions}
                                    isLoading={configsLoadStatus === "loading"}
                                    isDisabled={configsLoadStatus !== "success"}
                                    className="ms-1"
                                    styles={levelSelectStyles}
                                />
                            </div>
                        </CardHeader>
                        <CardBody>
                            <div className="d-flex gap-2 flex-wrap">
                                <Button
                                    type="button"
                                    color={isPaused ? "success" : "warning"}
                                    title={isPaused ? "Click to resume logging" : "Click to pause logging"}
                                    onClick={() => dispatch(adminLogsActions.isPausedToggled())}
                                >
                                    <Icon icon={isPaused ? "play" : "pause"} />
                                    {isPaused ? "Resume" : "Pause"}
                                </Button>
                                <Button
                                    type="button"
                                    color="danger"
                                    onClick={() => {
                                        eventsCollector.reportEvent("admin-logs", "clear");
                                        dispatch(adminLogsActions.logsSet([]));
                                    }}
                                >
                                    <Icon icon="cancel" />
                                    Clear
                                </Button>
                                <Button
                                    type="button"
                                    color={isMonitorTail ? "secondary" : "info"}
                                    onClick={() => dispatch(adminLogsActions.isMonitorTailToggled())}
                                >
                                    <Icon icon={isMonitorTail ? "pause" : "check"} />
                                    Monitor (tail -f)
                                </Button>
                                <AdminLogsExportButton />
                                <ButtonWithSpinner
                                    type="button"
                                    color="secondary"
                                    onClick={() => dispatch(adminLogsActions.isViewSettingOpenToggled())}
                                    isSpinning={configsLoadStatus === "loading"}
                                    icon="settings"
                                    disabled={configsLoadStatus !== "success"}
                                >
                                    Settings
                                </ButtonWithSpinner>
                                {isViewSettingOpen && <AdminLogsViewSettingsModal />}
                            </div>
                        </CardBody>
                    </Card>
                </div>
                <div className="flex-grow-1">
                    <Card>
                        <CardHeader className="d-flex justify-content-between">
                            <h4 className="mb-0">
                                <Icon icon="drive" />
                                Logs on disk
                            </h4>
                            <div className="d-flex align-items-center">
                                <Icon icon="logs" addon="arrow-filled-up" />
                                Min level:{" "}
                                {configsLoadStatus === "loading" ? (
                                    <LazyLoad active>
                                        <div>?????</div>
                                    </LazyLoad>
                                ) : (
                                    configs?.adminLogsConfig?.Logs?.CurrentMinLevel
                                )}
                            </div>
                        </CardHeader>
                        <CardBody>
                            <div className="d-flex gap-2 flex-wrap">
                                <Button
                                    type="button"
                                    color="secondary"
                                    onClick={() => dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled())}
                                >
                                    <Icon icon="download" />
                                    Download
                                </Button>
                                {isDownloadDiskLogsOpen && <AdminLogsDiskDownloadModal />}
                                <ButtonWithSpinner
                                    type="button"
                                    color="secondary"
                                    onClick={() => dispatch(adminLogsActions.isDiscSettingOpenToggled())}
                                    icon="settings"
                                    isSpinning={configsLoadStatus === "loading"}
                                    disabled={configsLoadStatus !== "success"}
                                >
                                    Settings
                                </ButtonWithSpinner>
                                {isDiscSettingOpen && <AdminLogsDiskSettingsModal />}
                            </div>
                        </CardBody>
                    </Card>
                </div>
            </div>
            <div className="d-flex gap-2">
                <div className="clearable-input flex-grow-1">
                    <Input
                        type="text"
                        placeholder="Search..."
                        value={localFilter}
                        onChange={(e) => handleFilterChange(e.target.value)}
                        className="pe-4"
                    />
                    {filter && (
                        <div className="clear-button">
                            <Button color="secondary" size="sm" onClick={() => handleFilterChange("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
                <Button
                    type="button"
                    color="secondary"
                    outline
                    onClick={() => dispatch(adminLogsActions.isAllExpandedToggled())}
                >
                    <Icon icon={isAllExpanded ? "collapse-vertical" : "expand-vertical"} />
                    {isAllExpanded ? "Collapse all" : "Expand all"}
                </Button>
                <Button
                    type="button"
                    color="secondary"
                    outline
                    onClick={() => dispatch(adminLogsActions.isDisplaySettingsOpenToggled())}
                >
                    <Icon icon="settings" />
                    Display settings
                </Button>
                {isDisplaySettingsOpen && <AdminLogsDisplaySettingsModal />}
            </div>
            <div className="flex-grow-1">
                <SizeGetter
                    isHeighRequired
                    render={(size) => <AdminLogsVirtualList availableHeightInPx={size.height} />}
                />
            </div>
        </div>
    );
}

const levelSelectStyles: StylesConfig = {
    control: (base) => ({
        ...base,
        minHeight: 22,
        height: 22,
        lineHeight: 1,
        minWidth: "fit-content",
    }),
    input: (base) => ({
        ...base,
        margin: 0,
    }),
    placeholder: (base) => ({
        ...base,
        height: 22,
    }),
    singleValue: (base) => ({
        ...base,
        height: 14,
    }),
    indicatorsContainer: () => ({
        padding: 0,
        paddingRight: 3,
    }),
    dropdownIndicator: (base) => ({
        ...base,
        padding: 0,
        paddingRight: 3,
        paddingBottom: 3,
    }),
    clearIndicator: (base) => ({
        ...base,
        padding: 0,
    }),
    menu: (base) => ({
        ...base,
        width: "fit-content",
    }),
    menuList: (base) => ({
        ...base,
        width: "fit-content",
    }),
    loadingIndicator: (base) => ({
        ...base,
        display: "none",
    }),
};
