import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import React from "react";
import { DropdownItem } from "reactstrap";

export default function StudioSearchServerResults(props: {
    serverColumnRef: React.RefObject<HTMLDivElement>;
    hasServerMatch: boolean;
    serverResults: StudioSearchResult["server"];
    activeItem?: StudioSearchResultItem;
}) {
    const { serverColumnRef, hasServerMatch, serverResults, activeItem } = props;

    if (!hasServerMatch) {
        return null;
    }

    return (
        <div className="col-md-4 col-sm-12 studio-search__server-col p-0" ref={serverColumnRef}>
            <DropdownItem header className="studio-search__server-col__header--sticky">
                <span className="small-label">Server</span>
            </DropdownItem>
            <div className="studio-search__server-col__group">
                {serverResults.map((item) => (
                    <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} isCapitalized />
                ))}
            </div>
        </div>
    );
}
