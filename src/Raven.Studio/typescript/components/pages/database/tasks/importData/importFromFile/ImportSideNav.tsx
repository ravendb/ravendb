import React from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { SectionNavItem, sectionNav } from "./importFromFileNav";

interface ImportSideNavProps {
    activeSectionId: string | null;
    onSelectSection: (id: string) => void;
}

export default function ImportSideNav({ activeSectionId, onSelectSection }: ImportSideNavProps) {
    return (
        <nav className="import-side-nav align-self-start">
            {sectionNav.map((item, index) => (
                <React.Fragment key={item.id}>
                    {index > 0 && <hr />}
                    <ImportSideNavItem
                        item={item}
                        activeSectionId={activeSectionId}
                        onSelectSection={onSelectSection}
                    />
                </React.Fragment>
            ))}
        </nav>
    );
}

interface ImportSideNavItemProps {
    item: SectionNavItem;
    activeSectionId: string | null;
    onSelectSection: (id: string) => void;
}

function ImportSideNavItem({ item, activeSectionId, onSelectSection }: ImportSideNavItemProps) {
    return (
        <>
            <button
                type="button"
                className={classNames("import-side-nav-item", {
                    active: activeSectionId === item.id,
                })}
                onClick={() => onSelectSection(item.id)}
            >
                <Icon icon={item.icon} margin="m-0" /> {item.label}
            </button>
            {item.children?.map((child) => (
                <button
                    key={child.id}
                    type="button"
                    className={classNames("import-side-nav-item import-side-nav-subitem", {
                        active: activeSectionId === child.id,
                    })}
                    onClick={() => onSelectSection(child.id)}
                >
                    {child.label}
                </button>
            ))}
        </>
    );
}
