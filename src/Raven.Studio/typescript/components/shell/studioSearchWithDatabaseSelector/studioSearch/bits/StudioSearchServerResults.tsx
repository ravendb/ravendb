import Dropdown from "react-bootstrap/Dropdown";
import StudioSearchDropdownItem from "../bits/StudioSearchResultItem";
import { StudioSearchResult, StudioSearchResultItem } from "../studioSearchTypes";
import React from "react";

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
        <div className="col-md-5 col-sm-12 studio-search__server-col p-0" ref={serverColumnRef}>
            <Dropdown.Header className="studio-search__server-col__header--sticky">
                <span className="small-label">Server</span>
            </Dropdown.Header>
            <div className="studio-search__server-col__group">
                {serverResults.map((item) => (
                    <StudioSearchDropdownItem key={item.id} item={item} activeItemId={activeItem?.id} />
                ))}
            </div>
        </div>
    );
}
