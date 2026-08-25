import "./expired.css";
import quillLogo from "@/components/brand/quill-logo.svg?raw";

// Inline svg rather than an <img>, so the logo keeps following the theme via currentColor.
document.getElementById("logo")?.insertAdjacentHTML("afterbegin", quillLogo);

const RESET_AFTER_MS = 2000;

// The icon swaps rather than the button's label, so the button keeps its width; the wording goes to the
// status line beside the step title, which is also what a screen reader announces.
document.querySelectorAll<HTMLButtonElement>("[data-copies]").forEach((button) => {
    const commandId = button.dataset.copies ?? "";
    const command = document.getElementById(commandId);
    if (command === null) {
        return;
    }

    const status = document.querySelector<HTMLElement>(`[data-status-for="${commandId}"]`);
    const idleIcon = button.querySelector<SVGElement>("[data-icon='idle']");
    const doneIcon = button.querySelector<SVGElement>("[data-icon='done']");
    let resetTimer: number | undefined;

    const flash = (message: string, copied: boolean) => {
        idleIcon?.classList.toggle("hidden", copied);
        doneIcon?.classList.toggle("hidden", copied === false);
        if (status !== null) {
            status.textContent = message;
        }

        window.clearTimeout(resetTimer);
        resetTimer = window.setTimeout(() => {
            idleIcon?.classList.remove("hidden");
            doneIcon?.classList.add("hidden");
            if (status !== null) {
                status.textContent = "";
            }
        }, RESET_AFTER_MS);
    };

    // Selecting the command is only worth doing when the copy did not happen: it is what makes
    // "Press Ctrl+C" actionable on an appliance browsed over plain http, where there is no clipboard
    // API. On the path that worked it would just be a highlight over text the user already has.
    const offerManualCopy = () => {
        const range = document.createRange();
        range.selectNodeContents(command);
        const selection = getSelection();
        selection?.removeAllRanges();
        selection?.addRange(range);
        flash("Press Ctrl+C", false);
    };

    button.addEventListener("click", () => {
        if (navigator.clipboard?.writeText === undefined) {
            offerManualCopy();
            return;
        }

        navigator.clipboard.writeText(command.textContent ?? "").then(() => flash("Copied", true), offerManualCopy);
    });
});
