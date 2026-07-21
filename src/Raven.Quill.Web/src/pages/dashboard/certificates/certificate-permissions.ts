import { z } from "zod";
import type { DatabaseAccess } from "@/api/generated/server-api";

export const permissionRowSchema = z.object({
    database: z.string(),
    access: z.enum(["Admin", "ReadWrite", "Read"]),
});

export type PermissionRow = z.infer<typeof permissionRowSchema>;

// Row validation lives here rather than at field level so dialogs can skip it
// when the selected clearance hides the permission rows.
export function reportPermissionRowIssues(
    rows: PermissionRow[],
    ctx: z.RefinementCtx,
    basePath: (string | number)[] = [],
) {
    rows.forEach((row, index) => {
        if (row.database === "") {
            ctx.addIssue({ code: "custom", path: [...basePath, index, "database"], message: "Required" });
            return;
        }

        if (rows.findIndex((other) => other.database === row.database) !== index) {
            ctx.addIssue({ code: "custom", path: [...basePath, index, "database"], message: "Already listed" });
        }
    });
}

export function toPermissionsRecord(rows: PermissionRow[]): Record<string, DatabaseAccess> {
    return Object.fromEntries(rows.map((row) => [row.database, row.access]));
}
