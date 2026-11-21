import "./DestinationPanel.scss";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import ConnectionTestErrorModal from "components/common/connectionTests/ConnectionTestErrorModal";
import RichAlert from "components/common/RichAlert";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import DestinationTestButton from "./DestinationTestButton";
import { useDestinationTest } from "./useDestinationTest";
import { DestinationType, FormDestinations } from "./utils/formDestinationsTypes";
import { formDestinationsUtils } from "./utils/formDestinationsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DestinationPanelProps {
    type: DestinationType;
    isDisabled?: boolean;
    allowedDestinations?: backupOptions[];
    onEdit: () => void;
    onDelete: () => void;
    onStateChange?: (isEnabled: boolean) => void;
}

export default function DestinationPanel({
    type,
    isDisabled,
    allowedDestinations,
    onEdit,
    onDelete,
    onStateChange,
}: DestinationPanelProps) {
    const { control, formState } = useFormContext<FormDestinations>();
    const destinations = useWatch({ control, name: "destinations" });
    const hasInvalidSettings = !!formState.errors.destinations?.[type];
    const asyncTest = useDestinationTest(type);
    const t = useStudioTranslation("formDestinations");

    const typeLabel = t(`types.${type}`);
    const isTestAvailable = formDestinationsUtils.isCredentialsTestAvailable(type, destinations[type]);

    return (
        <RichPanel className="destination-panel" aria-label={typeLabel}>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>
                        <Icon icon={formDestinationsUtils.DESTINATION_TYPE_INFO[type].icon} />
                        {typeLabel}
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    {isTestAvailable && <DestinationTestButton asyncTest={asyncTest} />}
                    {onStateChange && (
                        <DestinationStateDropdown
                            type={type}
                            isDestinationDisabled={!destinations[type].isEnabled}
                            isDisabled={isDisabled}
                            allowedDestinations={allowedDestinations}
                            onStateChange={onStateChange}
                        />
                    )}
                    <Button
                        variant="secondary"
                        onClick={onEdit}
                        disabled={isDisabled}
                        title={t("panel.edit", { destination: typeLabel })}
                    >
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button
                        variant="danger"
                        onClick={onDelete}
                        disabled={isDisabled}
                        title={t("panel.remove", { destination: typeLabel })}
                    >
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails className="flex-wrap">
                {formDestinationsUtils.getDestinationDetails(type, destinations, t).map((detail) => (
                    <RichPanelDetailItem key={detail.label} label={detail.label} className="text-break">
                        {detail.value}
                    </RichPanelDetailItem>
                ))}
            </RichPanelDetails>
            {hasInvalidSettings && (
                <div className="px-3 pb-3">
                    <RichAlert variant="danger">{t("panel.invalid")}</RichAlert>
                </div>
            )}
            {asyncTest.isErrorModalOpen && (
                <ConnectionTestErrorModal error={asyncTest.result.Error} onClose={asyncTest.closeErrorModal} />
            )}
        </RichPanel>
    );
}

interface DestinationStateDropdownProps {
    type: DestinationType;
    isDestinationDisabled: boolean;
    isDisabled?: boolean;
    allowedDestinations?: backupOptions[];
    onStateChange: (isEnabled: boolean) => void;
}

function DestinationStateDropdown({
    type,
    isDestinationDisabled,
    isDisabled,
    allowedDestinations,
    onStateChange,
}: DestinationStateDropdownProps) {
    const t = useStudioTranslation("formDestinations");

    const isEnableBlocked =
        isDestinationDisabled && !formDestinationsUtils.isDestinationAllowed(type, allowedDestinations);

    return (
        <ConditionalPopover conditions={{ isActive: isEnableBlocked, message: t("panel.enableBlockedByAdmin") }}>
            <Dropdown>
                <Dropdown.Toggle
                    variant={isDestinationDisabled ? "warning" : "secondary"}
                    disabled={isDisabled || isEnableBlocked}
                    title={t("panel.state", { destination: t(`types.${type}`) })}
                >
                    {isDestinationDisabled ? t("panel.stateDisabled") : t("panel.stateEnabled")}
                </Dropdown.Toggle>
                <Dropdown.Menu>
                    <Dropdown.Item onClick={() => onStateChange(true)}>
                        <Icon icon="play" color="success" />
                        {t("panel.enable")}
                    </Dropdown.Item>
                    <Dropdown.Item onClick={() => onStateChange(false)}>
                        <Icon icon="stop" color="danger" />
                        {t("panel.disable")}
                    </Dropdown.Item>
                </Dropdown.Menu>
            </Dropdown>
        </ConditionalPopover>
    );
}
