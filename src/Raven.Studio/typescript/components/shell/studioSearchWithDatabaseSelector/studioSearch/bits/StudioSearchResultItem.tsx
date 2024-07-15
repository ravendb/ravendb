import { Icon } from "components/common/Icon";
import StudioSearchFuzzyHighlightedText from "../bits/StudioSearchFuzzyHighlightedText";
import { StudioSearchResultItem } from "../studioSearchTypes";
import React from "react";
import { DropdownItem } from "reactstrap";

interface StudioSearchDropdownItemProps {
    item: StudioSearchResultItem;
    activeItemId?: string;
}

export default function StudioSearchDropdownItem({ item, activeItemId }: StudioSearchDropdownItemProps) {
    return (
        <DropdownItem
            onClick={item.onSelected}
            className="d-flex align-items-center studio-search__dropdown-item"
            active={activeItemId === item.id}
            id={item.id}
        >
            <Icon icon={item.icon} />
            <div className="lh-1">
                {item.innerActionText ? (
                    <>
                        <StudioSearchFuzzyHighlightedText
                            text={item.innerActionText}
                            indices={item.innerActionIndices}
                        />
                        <br />
                        <span className="studio-search__route">{item.route}</span>
                    </>
                ) : (
                    <StudioSearchFuzzyHighlightedText text={item.text} indices={item.indices} />
                )}
            </div>
        </DropdownItem>
    );
}
