import { createRoot } from "react-dom/client";
import { LiveApp } from "@/live-app";
import { PreviewApp } from "@/preview-app";
import { readConfig, readMode } from "@/widget-config";
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

    if (readMode(window.location.search) === "preview") {
        createRoot(container).render(<PreviewApp />);
        return;
    }

    try {
        const config = readConfig(document);
        createRoot(container).render(<LiveApp config={config} />);
    } catch {
        // A malformed or absent config block means the shell was served wrong; a blank frame would just
        // look like a hung widget, so say something the visitor can act on.
        renderFatal(container, "This assistant is unavailable right now.");
    }
}

mount();
