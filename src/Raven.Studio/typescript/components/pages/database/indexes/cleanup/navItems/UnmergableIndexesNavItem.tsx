import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import { NavItem, Card, Badge } from "reactstrap";

const unmergableIndexesImg = require("Content/img/pages/indexCleanup/unmergable-indexes.svg");

interface UnmergableIndexesNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    unmergable: UseIndexCleanupResult["unmergable"];
}

export default function UnmergableIndexesNavItem({ carousel, unmergable }: UnmergableIndexesNavItemProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <NavItem>
            <Card
                className={classNames("p-3", "card-tab", { active: carousel.activeTab === 3 })}
                onClick={() => carousel.setActiveTab(3)}
            >
                <img src={unmergableIndexesImg} alt="Unmergeable indexes" />
                <Badge className="rounded-pill fs-5" color={unmergable.data.length !== 0 ? "primary" : "secondary"}>
                    {hasIndexCleanup ? unmergable.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    Unmergeable
                    <br />
                    indexes
                </h4>
            </Card>
        </NavItem>
    );
}
