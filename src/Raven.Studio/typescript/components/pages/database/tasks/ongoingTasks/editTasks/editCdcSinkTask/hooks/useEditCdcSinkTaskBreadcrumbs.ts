import { CdcActiveTable } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    castToRootTablePath,
    castToLinkedTablePath,
    castToEmbeddedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import assertUnreachable from "components/utils/assertUnreachable";
import { useFormContext, useWatch } from "react-hook-form";

type PathFieldName = Extract<
    keyof EditCdcSinkTaskFormData | keyof EditCdcSinkTaskFormData["tables"][number],
    "tables" | "linkedTables" | "embeddedTables"
>;

export type EditCdcSinkTaskBreadcrumbItem = CdcActiveTable & {
    isActive?: boolean;
    label?: string;
};

export default function useEditCdcSinkTaskBreadcrumbs(path: CdcActiveTable["path"]): EditCdcSinkTaskBreadcrumbItem[] {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const parts = path.split(".");
    const breadcrumbItems: EditCdcSinkTaskBreadcrumbItem[] = [];

    for (let i = 0; i < parts.length; i += 2) {
        const singleFieldWithNumber = `${parts[i]}.${parts[i + 1]}`;
        const lastPath = breadcrumbItems[breadcrumbItems.length - 1]?.path;
        const path = lastPath ? `${lastPath}.${singleFieldWithNumber}` : singleFieldWithNumber;

        breadcrumbItems.push(getActiveTableFieldName(parts[i] as PathFieldName, path));
    }

    const breadcrumbSourceTableNames = breadcrumbItems.map((item) => `${item.path}.sourceTableName` as const);
    const sourceTableNames = useWatch({ control, name: breadcrumbSourceTableNames });

    return breadcrumbItems.map((item, idx) => ({
        ...item,
        label: sourceTableNames[idx] || "Unassigned table",
        isActive: idx === breadcrumbItems.length - 1,
    }));
}

function getActiveTableFieldName(fieldName: PathFieldName, path: string): CdcActiveTable {
    switch (fieldName) {
        case "tables":
            return { type: "root", path: castToRootTablePath(path) };
        case "linkedTables":
            return { type: "linked", path: castToLinkedTablePath(path) };
        case "embeddedTables":
            return { type: "embedded", path: castToEmbeddedTablePath(path) };
        default:
            assertUnreachable(fieldName);
    }
}
