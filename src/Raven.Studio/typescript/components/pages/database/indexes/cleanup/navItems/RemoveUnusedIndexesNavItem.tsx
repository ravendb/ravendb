import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { NavItem, Card, Badge } from "reactstrap";

const removeUnusedImg = require("Content/img/pages/indexCleanup/remove-unused.svg");

interface RemoveUnusedIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    unused: UseIndexCleanupResult["unused"];
}

export default function RemoveUnusedIndexesNavItem({ carousel, unused }: RemoveUnusedIndexesNavItemProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <NavItem>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 2 })}
                onClick={() => carousel.setActiveTab(2)}
            >
                <img src={removeUnusedImg} alt="Remove unused indexes " />
                <Badge className="rounded-pill fs-5" color={unused.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? unused.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    Remove <br />
                    unused indexes
                </h4>
            </Card>
        </NavItem>
    );
}
