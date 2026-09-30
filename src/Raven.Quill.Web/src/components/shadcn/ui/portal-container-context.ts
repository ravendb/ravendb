import { createContext, useContext } from "react";

/**
 * The element that floating popups (Autocomplete, Combobox, …) should portal into.
 *
 * Radix modal layers (Dialog, Sheet) set `pointer-events: none` on everything
 * outside their content node, so a popup portaled to `<body>` — the Base UI
 * default — renders but can't be clicked. Modal wrappers publish their content
 * node through this context; popups read it and portal inside it, which restores
 * pointer interaction (and keeps the popup within the modal's focus scope).
 *
 * Outside any provider the value is `null` and popups fall back to `<body>`.
 */
const PortalContainerContext = createContext<HTMLElement | null>(null);

export function usePortalContainer() {
    return useContext(PortalContainerContext);
}

export default PortalContainerContext;
