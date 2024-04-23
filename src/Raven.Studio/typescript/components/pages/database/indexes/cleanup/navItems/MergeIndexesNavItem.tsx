import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { NavItem, Card, Badge } from "reactstrap";

const mergeIndexesImg = require("Content/img/pages/indexCleanup/merge-indexes.svg");

interface MergeIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    mergable: UseIndexCleanupResult["mergable"];
}

export default function MergeIndexesNavItem({ carousel, mergable }: MergeIndexesNavItemProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <NavItem>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 0 })}
                onClick={() => carousel.setActiveTab(0)}
            >
                <img src={mergeIndexesImg} alt="Merge indexes" />
                <Badge className="rounded-pill fs-5" color={mergable.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? mergable.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    Merge
                    <br />
                    indexes
                </h4>
            </Card>
        </NavItem>
    );
}
