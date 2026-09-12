import { z } from "zod";
import type { DatabaseAccess } from "@/api/generated/server-api";

// The server contract also has Read, which Quill never grants.
export const GRANTABLE_DATABASE_ACCESS = ["Admin", "ReadWrite"] as const satisfies readonly DatabaseAccess[];

export const permissionRowSchema = z.object({
    database: z.string(),
    access: z.enum(["Admin", "ReadWrite", "Read"] as const satisfies readonly DatabaseAccess[]),
});

export type PermissionRow = z.infer<typeof permissionRowSchema>;

function isGrantableAccess(access: DatabaseAccess): boolean {
    return GRANTABLE_DATABASE_ACCESS.some((grantable) => grantable === access);
}

// Row validation lives here rather than at field level so dialogs can skip it
// when the selected clearance hides the permission rows.
export function reportPermissionRowIssues(
    rows: PermissionRow[],
    ctx: z.RefinementCtx,
    basePath: (string | number)[] = [],
) {
    rows.forEach((row, index) => {
        if (!isGrantableAccess(row.access)) {
            ctx.addIssue({
                code: "custom",
                path: [...basePath, index, "access"],
                message: "Read-only access is no longer supported. Pick another level.",
            });
        }

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
