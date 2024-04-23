import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { NavItem, Card, Badge } from "reactstrap";

const removeSubindexesImg = require("Content/img/pages/indexCleanup/remove-subindexes.svg");

interface RemoveSubindexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    surpassing: UseIndexCleanupResult["surpassing"];
}

export default function RemoveSubindexesNavItem({ carousel, surpassing }: RemoveSubindexesNavItemProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <NavItem>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 1 })}
                onClick={() => carousel.setActiveTab(1)}
            >
                <img src={removeSubindexesImg} alt="Remove subindexes" />
                <Badge className="rounded-pill fs-5" color={surpassing.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? surpassing.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    Remove
                    <br />
                    sub-indexes
                </h4>
            </Card>
        </NavItem>
    );
}
