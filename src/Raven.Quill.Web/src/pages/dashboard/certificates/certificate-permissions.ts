import { z } from "zod";
import type { DatabaseAccess } from "@/api/generated/server-api";

export const permissionRowSchema = z.object({
    database: z.string().min(1, "Required"),
    access: z.enum(["Admin", "ReadWrite", "Read"]),
});

export type PermissionRow = z.infer<typeof permissionRowSchema>;

export function reportDuplicateDatabases(rows: PermissionRow[], ctx: z.RefinementCtx) {
    rows.forEach((row, index) => {
        const isDuplicate = row.database !== "" && rows.findIndex((other) => other.database === row.database) !== index;
        if (isDuplicate) {
            ctx.addIssue({ code: "custom", path: [index, "database"], message: "Already listed" });
        }
    });
}

export function toPermissionsRecord(rows: PermissionRow[]): Record<string, DatabaseAccess> {
    return Object.fromEntries(rows.map((row) => [row.database, row.access]));
}
