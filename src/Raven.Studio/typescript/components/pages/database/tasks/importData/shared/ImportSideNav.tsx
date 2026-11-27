import React from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";

interface ImportSideNavProps {
    items: SectionNavItem[];
    activeSectionId: string | null;
    onSelectSection: (id: string) => void;
}

export default function ImportSideNav({ items, activeSectionId, onSelectSection }: ImportSideNavProps) {
    return (
        <nav className="import-side-nav align-self-start">
            {items.map((item, index) => (
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
    const t = useStudioTranslation("importData");

    return (
        <>
            <button
                type="button"
                className={classNames("import-side-nav-item", {
                    active: activeSectionId === item.id,
                })}
                onClick={() => onSelectSection(item.id)}
            >
                <Icon icon={item.icon} margin="m-0" /> {t(item.label)}
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
                    {t(child.label)}
                </button>
            ))}
        </>
    );
}
