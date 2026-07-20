import React from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { AnalysisSectionEntry } from "./AnalysisSectionsContext";
import "./AnalysisNavRail.scss";
import Badge from "react-bootstrap/Badge";

export interface ScopeItem<TScope extends string> {
    value: TScope;
    label: string;
    icon: IconName;
    // omitted when the count would be misleading on the generic scope tab (it then lives on the scope's
    // selector instead - e.g. node/database counts depend on which node/database is selected)
    count?: number;
}

interface AnalysisNavRailProps<TScope extends string> {
    scopeItems: ScopeItem<TScope>[];
    selectedScope: TScope;
    onSelectScope: (scope: TScope) => void;
    globalControls?: React.ReactNode;
    scopeControls?: React.ReactNode;
    sections: AnalysisSectionEntry[];
    activeSectionId: string | null;
    onSelectSection: (id: string) => void;
}

export default function AnalysisNavRail<TScope extends string>({
    scopeItems,
    selectedScope,
    onSelectScope,
    globalControls,
    scopeControls,
    sections,
    activeSectionId,
    onSelectSection,
}: AnalysisNavRailProps<TScope>) {
    return (
        <nav className="analysis-rail rounded" aria-label="Analysis navigation">
            <div className="analysis-rail-scope" role="tablist">
                {scopeItems.map((item) => (
                    <button
                        key={item.value}
                        type="button"
                        role="tab"
                        aria-selected={selectedScope === item.value}
                        className={classNames("analysis-rail-scope-btn", { active: selectedScope === item.value })}
                        onClick={() => onSelectScope(item.value)}
                    >
                        <Icon icon={item.icon} margin="m-0" />
                        <span className="flex-grow-1">{item.label}</span>
                        {item.count != null && (
                            <Badge bg="secondary" pill>
                                {item.count}
                            </Badge>
                        )}
                    </button>
                ))}
            </div>

            {scopeControls && (
                <>
                    <div className="analysis-rail-divider" />
                    <div className="analysis-rail-controls">{scopeControls}</div>
                </>
            )}

            {sections.length > 0 && (
                <>
                    <div className="analysis-rail-divider" />
                    <ul className="analysis-rail-sections d-none d-xl-flex">
                        {groupSections(sections).map((group, groupIndex) => (
                            <React.Fragment key={group.label ?? `__ungrouped-${groupIndex}`}>
                                {group.label && (
                                    <li className="analysis-rail-group-label" aria-hidden="true">
                                        {group.label}
                                    </li>
                                )}
                                {group.items.map((section) => (
                                    <li key={section.id}>
                                        <button
                                            type="button"
                                            className={classNames("analysis-rail-section-btn", {
                                                active: activeSectionId === section.id,
                                            })}
                                            onClick={() => onSelectSection(section.id)}
                                        >
                                            {section.label}
                                        </button>
                                    </li>
                                ))}
                            </React.Fragment>
                        ))}
                    </ul>
                </>
            )}
            {globalControls && (
                <>
                    <div className="analysis-rail-divider" />
                    <div className="analysis-rail-controls">{globalControls}</div>
                </>
            )}
        </nav>
    );
}

interface SectionGroup {
    label?: string;
    items: AnalysisSectionEntry[];
}

// Collapse the flat, DOM-ordered section list into consecutive runs sharing the same group, so the
// nav rail can render a heading per group. Sections without a group (cluster/node scopes) stay flat.
function groupSections(sections: AnalysisSectionEntry[]): SectionGroup[] {
    const groups: SectionGroup[] = [];
    sections.forEach((section) => {
        const last = groups[groups.length - 1];
        if (last && last.label === section.group) {
            last.items.push(section);
        } else {
            groups.push({ label: section.group, items: [section] });
        }
    });
    return groups;
}
