import { createContext, useContext } from "react";

/**
 * Overlay nesting for forms: each sheet/dialog opens a child scope so it sees only its own forms.
 * Paths are strings, not arrays, so hook deps compare by value.
 */
const SCOPE_SEPARATOR = "/";

export const ROOT_SCOPE_PATH = "";

export const UnsavedChangesScopeContext = createContext(ROOT_SCOPE_PATH);

export function useUnsavedChangesScopePath() {
    return useContext(UnsavedChangesScopeContext);
}

export function toChildScopePath(parentPath: string, scopeId: string) {
    return parentPath === ROOT_SCOPE_PATH ? scopeId : `${parentPath}${SCOPE_SEPARATOR}${scopeId}`;
}

export function isInScope(formScopePath: string, scopePath: string) {
    return formScopePath === scopePath || formScopePath.startsWith(`${scopePath}${SCOPE_SEPARATOR}`);
}
