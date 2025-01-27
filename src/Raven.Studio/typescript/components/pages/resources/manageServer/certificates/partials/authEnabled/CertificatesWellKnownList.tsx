import { Icon } from "components/common/Icon";
import {
    RichPanel,
    RichPanelHeader,
    RichPanelName,
    RichPanelDetails,
    RichPanelDetailItem,
    RichPanelStatus,
} from "components/common/RichPanel";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useAppSelector } from "components/store";
import { Badge } from "reactstrap";

export default function CertificatesWellKnownList() {
    const wellKnownAdminCerts = useAppSelector(certificatesSelectors.wellKnownAdminCerts);
    const wellKnownIssuers = useAppSelector(certificatesSelectors.wellKnownIssuers);

    return (
        <div className="vstack gap-2">
            <WellKnownItem
                title="Well known admin certificates defined by system administrator"
                wellKnownThumbprints={wellKnownAdminCerts}
            />
            <WellKnownItem
                title="Well known issuer certificates defined by system administrator"
                wellKnownThumbprints={wellKnownIssuers}
            />
        </div>
    );
}

interface WellKnownItemProps {
    wellKnownThumbprints: string[];
    title: string;
}

function WellKnownItem({ title, wellKnownThumbprints }: WellKnownItemProps) {
    const nameOrThumbprintFilter = useAppSelector(certificatesSelectors.nameOrThumbprintFilter);
    const clearanceFilter = useAppSelector(certificatesSelectors.clearanceFilter);

    if (wellKnownThumbprints.length === 0) {
        return null;
    }

    if (
        wellKnownThumbprints.some(
            (thumbprint) => !thumbprint.toLowerCase().includes(nameOrThumbprintFilter.toLowerCase())
        )
    ) {
        return null;
    }

    if (clearanceFilter.length > 0 && !clearanceFilter.includes("Admin")) {
        return null;
    }

    return (
        <RichPanel className="flex-row with-status ">
            <RichPanelStatus color="info">Well known</RichPanelStatus>
            <div className="flex-grow">
                <RichPanelHeader>
                    <div>
                        <RichPanelName className="d-flex align-items-center w-100">
                            <Icon icon="certificate" color="primary" />
                            {title}
                        </RichPanelName>
                        {wellKnownThumbprints.join(", ")}
                    </div>
                </RichPanelHeader>
                <RichPanelDetails>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="user" />
                                Security clearance
                            </>
                        }
                    >
                        Cluster Admin
                    </RichPanelDetailItem>
                    <RichPanelDetailItem
                        label={
                            <>
                                <Icon icon="database" />
                                Database permissions
                            </>
                        }
                    >
                        <Badge color="faded-success" pill>
                            <Icon icon="user" />
                            All
                        </Badge>
                    </RichPanelDetailItem>
                </RichPanelDetails>
            </div>
        </RichPanel>
    );
}
