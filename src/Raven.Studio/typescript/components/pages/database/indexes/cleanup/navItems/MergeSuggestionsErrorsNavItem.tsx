import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { UseIndexCleanupResult } from "components/pages/database/indexes/cleanup/useIndexCleanup";
import { useAppSelector } from "components/store";
import React from "react";
import Badge from "react-bootstrap/Badge";
import Card from "react-bootstrap/Card";
import Nav from "react-bootstrap/Nav";

const mergeSuggestionErrors = require("Content/img/pages/indexCleanup/merge-suggestion-errors.svg");

interface MergeSuggestionsErrorsNavItemProps {
    carousel: UseIndexCleanupResult["carousel"];
    errors: UseIndexCleanupResult["errors"];
}

export default function MergeSuggestionsErrorsNavItem({ carousel, errors }: MergeSuggestionsErrorsNavItemProps) {
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    return (
        <Nav.Item>
            <Card
                className={classNames("p-3", "card-tab", {
                    active: carousel.activeTab === 4,
                })}
                onClick={() => carousel.setActiveTab(4)}
            >
                <img src={mergeSuggestionErrors} alt="Merge suggestion errors" />
                <Badge className="rounded-pill fs-5" bg="primary">
                    {hasIndexCleanup ? errors.data.length : "?"}
                </Badge>
                <h4 className="text-center">
                    Merge suggestions
                    <br />
                    errors
                </h4>
            </Card>
        </Nav.Item>
    );
}
