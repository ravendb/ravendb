import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import React from "react";
import { DropdownItem } from "reactstrap";
import classNames from "classnames";

export default function StudioSearchServerResults(props: {
    serverColumnRef: React.RefObject<HTMLDivElement>;
    hasServerMatch: boolean;
    serverResults: StudioSearchResult["server"];
    activeItem?: StudioSearchResultItem;
    colWidth: number;
}) {
    const { serverColumnRef, hasServerMatch, serverResults, activeItem, colWidth } = props;

    if (!hasServerMatch) {
        return null;
    }

    return (
        <div className={classNames(`col-md-${colWidth} studio-search__server-col p-0`)} ref={serverColumnRef}>
            <DropdownItem header className="studio-search__server-col__header--sticky">
                <span className="small-label">Server</span>
            </DropdownItem>
            <div className="studio-search__server-col__group">
                {serverResults.map((item) => (
                    <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
                ))}
            </div>
        </div>
    );
}
