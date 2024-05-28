import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";
import React from "react";
import feedback from "viewmodels/shell/feedback";
import app from "durandal/app";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";

export function AboutFooter() {
    const clientVersion = useAppSelector(clusterSelectors.clientVersion);
    const serverFullVersion = useAppSelector(clusterSelectors.serverVersion)?.FullVersion ?? "n/a";

    const openSendFeedbackModal = () => {
        const dialog = new feedback(clientVersion, serverFullVersion);
        app.showBootstrapDialog(dialog);
    };

    return (
        <div className="hstack align-items-center gap-4 flex-wrap justify-content-center mb-4">
            <div className="hstack">
                <Button
                    color="info"
                    className="d-flex rounded-pill align-items-center py-1 ps-3 pe-4"
                    onClick={openSendFeedbackModal}
                >
                    <Icon icon="rocket" margin="me-2" className="fs-2"></Icon>
                    <div className="text-start lh-1">
                        <div className="small">Help us improve</div>
                        <strong>Send Feedback</strong>
                    </div>
                </Button>
                <div className="d-flex align-item text-center ms-4">
                    <a href={aboutPageUrls.ravenDbHome} className="text-emphasis p-2" target="_blank">
                        <Icon icon="global" margin="m-0" />
                    </a>
                    <a href={aboutPageUrls.social.facebook} className="text-emphasis p-2" target="_blank">
                        <Icon icon="facebook" margin="m-0" />
                    </a>
                    <a href={aboutPageUrls.social.x} className="text-emphasis p-2" target="_blank">
                        <Icon icon="twitter" margin="m-0" />
                    </a>
                    <a href={aboutPageUrls.social.linkedIn} className="text-emphasis p-2" target="_blank">
                        <Icon icon="linkedin" margin="m-0" />
                    </a>
                </div>
            </div>
            <div className="small text-muted text-center">
                Copyright © 2009 - 2024 Hibernating Rhinos. All rights reserved.
            </div>
        </div>
    );
}
