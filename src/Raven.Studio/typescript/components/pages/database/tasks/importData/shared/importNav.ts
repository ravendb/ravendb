import IconName from "typings/server/icons";
import { TranslationKey } from "common/i18n/resources";

export type SectionLabelKey = Extract<TranslationKey<"importData">, `sections.${string}`>;

export interface SectionNavItem {
    id: string;
    label: SectionLabelKey;
    icon: IconName;
    children?: { id: string; label: SectionLabelKey }[];
}

export interface ErrorFieldTarget {
    path: string;
    sectionId: string;
}

export function getSectionIds(items: SectionNavItem[]): string[] {
    return items.flatMap((item) => [item.id, ...(item.children?.map((child) => child.id) ?? [])]);
}

export function getFirstErrorPath(errors: unknown, prefix = ""): string | null {
    if (!errors || typeof errors !== "object") {
        return null;
    }
    if ("message" in errors && typeof (errors as { message?: unknown }).message === "string") {
        return prefix;
    }
    for (const [key, value] of Object.entries(errors)) {
        const path = prefix ? `${prefix}.${key}` : key;
        const found = getFirstErrorPath(value, path);
        if (found) {
            return found;
        }
    }
    return null;
}

export function getSectionIdForErrorPath(errorPath: string, targets: ErrorFieldTarget[]): string | null {
    return targets.find((x) => errorPath.startsWith(x.path))?.sectionId ?? null;
}
