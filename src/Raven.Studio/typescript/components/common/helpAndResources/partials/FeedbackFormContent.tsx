import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import { Button, Input } from "reactstrap";
import { useRavenLink } from "hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { todo } from "common/developmentHelper";

interface FeedbackFormContentProps {}

export function FeedbackFormContent() {
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const gitHubCommunityUrl = useRavenLink({ hash: "ITXUEA" });
    const cloudRequestSupport = useRavenLink({ hash: "2YGOL1" });
    const onPremiseRequestSupport = "https://ravendb.net/support/supportrequest?licenseId=" + licenseId;

    const requestSupportUrl = isCloud ? cloudRequestSupport : onPremiseRequestSupport;

    todo("Feature", "Damian", "Add logic to the feedback form");

    return (
        <>
            <ul className="action-menu__list">
                <Input placeholder="Your name" type="text" required />
                <Input placeholder="Your email" type="email" required />
                <Input placeholder="Message" type="textarea" rows={8} required />
                <div className="d-flex">
                    <FlexGrow />
                    <Button color="primary" className="rounded-pill">
                        <Icon icon="paperplane" /> Send feedback
                    </Button>
                </div>
            </ul>
            <div className="action-menu__footer">
                <small className="text-muted lh-1">
                    <Icon icon="github" />
                    Join our{" "}
                    <a href={gitHubCommunityUrl} target="_blank">
                        GitHub community
                    </a>
                </small>
                <small className="text-muted lh-1 mt-1">
                    <Icon icon="support" />
                    Need help?{" "}
                    <a href={requestSupportUrl} target="_blank">
                        Contact support
                    </a>
                </small>
            </div>
        </>
    );
}
