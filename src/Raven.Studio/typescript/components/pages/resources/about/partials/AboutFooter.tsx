import { Icon } from "components/common/Icon";
import React from "react";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";

export function AboutFooter() {
    return (
        <div className="hstack align-items-center gap-4 flex-wrap justify-content-center mb-4">
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
            <div className="small text-muted text-center">
                Copyright © 2009 - 2024 Hibernating Rhinos. All rights reserved.
            </div>
        </div>
    );
}
