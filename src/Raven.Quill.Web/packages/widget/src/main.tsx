import { createRoot } from "react-dom/client";
import { announceHostError } from "@/host-channel";
import { LiveApp } from "@/live-app";
import { PreviewApp } from "@/preview-app";
import { readConfigJson, resolveMount } from "@/widget-config";
import "@/widget.css";

const ROOT_ELEMENT_ID = "rq-root";

function renderFatal(container: HTMLElement, message: string) {
    container.textContent = message;
    container.setAttribute("role", "alert");
    container.className = "flex h-full items-center justify-center p-6 text-center text-sm";
}

function mount() {
    const container = document.getElementById(ROOT_ELEMENT_ID);
    if (container === null) throw new Error(`missing #${ROOT_ELEMENT_ID}`);

    const resolved = resolveMount(readConfigJson(document), window.location.search);
    switch (resolved.mode) {
        case "live":
            createRoot(container).render(<LiveApp config={resolved.config} />);
            return;
        case "preview":
            createRoot(container).render(<PreviewApp />);
            return;
        case "unusable":
            // A blank frame would just look like a hung widget, so say something the visitor can act on —
            // and tell the host, which would otherwise sit on its loader forever waiting for a `ready`
            // that never comes.
            renderFatal(container, "This assistant is unavailable right now.");
            announceHostError("widget config is unusable");
            return;
    }
}

mount();
