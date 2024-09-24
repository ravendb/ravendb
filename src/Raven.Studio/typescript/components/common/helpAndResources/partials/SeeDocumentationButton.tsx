import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import React from "react";
import router from "plugins/router";
import { useGetRavenLink } from "components/hooks/useRavenLink";
import { docsHashes } from "components/utils/docsHashes";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";
import genUtils from "common/generalUtils";

export default function SeeDocumentationButton() {
    const clientMajorVersion = getMajorVersion(useAppSelector(clusterSelectors.clientVersion));
    const getRavenLink = useGetRavenLink();

    const handleOpenDocumentation = () => {
        const singleRoute = genUtils.getSingleRoute(router.activeInstruction()?.config?.route);
        const docsHash = docsHashes[singleRoute];

        const getLink = () => {
            if (!docsHash || docsHash === "MISSING_DOCS") {
                return `https://ravendb.net/docs/article-page/${clientMajorVersion}`;
            }

            return getRavenLink({ hash: docsHash });
        };

        window.open(getLink(), "_blank");
    };

    return (
        <li
            className="action-menu__list-item"
            role="button"
            title="See documentation"
            onClick={handleOpenDocumentation}
        >
            <Icon icon="document2" margin="m-0" />
            See documentation
            <FlexGrow />
            <Icon icon="newtab" margin="m-0" />
        </li>
    );
}

function getMajorVersion(version: string) {
    return version.slice(0, 3);
}
