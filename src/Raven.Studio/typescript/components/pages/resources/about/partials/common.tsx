import { Col } from "reactstrap";
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
        <Col sm={7} className="mb-3">
            <div className="d-flex">
                <Icon icon={icon} className="fs-1" margin="me-3 mt-2" />
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
    gitHubDiscussions: "https://github.com/ravendb/ravendb/discussions",
    social: {
        facebook: "https://github.com/ravendb/ravendb/discussions",
        x: "https://twitter.com/ravendb",
        linkedIn: "https://www.linkedin.com/company/ravendb",
    },
    upgradeSupport: {
        onPremise: "http://ravendb.net/support",
        cloud: "https://cloud.ravendb.net/pricing#support-options",
    },
};
