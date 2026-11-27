import "./ImportPageLayout.scss";
import React, { ReactNode } from "react";
import { AboutViewHeading } from "components/common/AboutView";
import IconName from "typings/server/icons";
import ImportSideNav from "components/pages/database/tasks/importData/shared/ImportSideNav";
import { SectionNavItem } from "components/pages/database/tasks/importData/shared/importNav";
import { ImportPageScroll } from "components/pages/database/tasks/importData/shared/useImportPageScroll";

interface ImportPageLayoutProps {
    title: string;
    icon: IconName;
    backUrl: string;
    alert?: ReactNode;
    actions: ReactNode;
    banner?: ReactNode;
    navItems: SectionNavItem[];
    scroll: ImportPageScroll;
    children: ReactNode;
}

export default function ImportPageLayout({
    title,
    icon,
    backUrl,
    alert,
    actions,
    banner,
    navItems,
    scroll,
    children,
}: ImportPageLayoutProps) {
    return (
        <div className="import-page">
            <AboutViewHeading title={title} icon={icon} backUrl={backUrl} marginBottom={2} />
            {alert}
            <div className="my-4 d-flex align-items-center gap-3">{actions}</div>
            {banner}
            <div className="d-flex gap-4 import-page-body">
                <ImportSideNav
                    items={navItems}
                    activeSectionId={scroll.activeSectionId}
                    onSelectSection={scroll.selectSection}
                />
                <div className="flex-grow-1 import-page-content" ref={scroll.contentRef}>
                    {children}
                </div>
            </div>
        </div>
    );
}
