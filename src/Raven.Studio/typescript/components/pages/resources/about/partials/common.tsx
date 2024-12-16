import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import React, { ReactNode } from "react";
import IconName from "../../../../../../typings/server/icons";

interface OverallInfoItemProps {
    icon: IconName;
    label: string;
    children: string | ReactNode | ReactNode[];
}

export function OverallInfoItem(props: OverallInfoItemProps) {
    const { icon, label, children } = props;
    return (
        <Col sm={7}>
            <div className="d-flex">
                <Icon icon={icon} className="fs-1" margin="me-3 mt-1" />
                <div className="vstack">
                    <small className="text-muted">{label}</small>
                    <strong className="fs-4 text-emphasis">{children}</strong>
                </div>
            </div>
        </Col>
    );
}

export interface ConnectivityStatus {
    connected: boolean;
    exception: string;
}

export const aboutPageUrls = {
    ravenDbHome: "https://ravendb.net",
    updateInstructions: "https://ravendb.net/l/LC9K4B",
    getLicense: "https://ravendb.net/buy",
    supportTerms: "https://ravendb.net/terms",
    cloudSupportTerms: "https://cloud.ravendb.net/terms",
    cloudPortal: "https://cloud.ravendb.net/portal",
    gitHubDiscussions: "https://ravendb.net/l/ITXUEA",
    discordServer: "https://ravendb.net/l/ZL8MVM",
    joinCommunity: "https://ravendb.net/l/FUNILW",
    supportRequest: (licenseId: string) => "https://ravendb.net/support/supportrequest?licenseId=" + licenseId,
    social: {
        facebook: "https://www.facebook.com/profile.php?id=100063550929698",
        x: "https://twitter.com/ravendb",
        linkedIn: "https://www.linkedin.com/company/ravendb",
    },
    upgradeSupport: {
        onPremise: "http://ravendb.net/support",
        cloud: "https://cloud.ravendb.net/pricing#support-options",
    },
};
