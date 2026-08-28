import "./expired.css";
import quillLogo from "@/components/brand/quill-logo.svg?raw";
import interLatin from "@fontsource-variable/inter/files/inter-latin-wght-normal.woff2?inline";

const inter = new FontFace("Inter Variable", `url(${interLatin})`, { weight: "100 900", display: "swap" });
document.fonts.add(inter);
void inter.load();

// Inline svg rather than an <img>, so the logo keeps following the theme via currentColor.
document.getElementById("logo")?.insertAdjacentHTML("afterbegin", quillLogo);

const RESET_AFTER_MS = 2000;

const command = document.getElementById("command");
const copyButton = document.getElementById("copy");

if (command !== null && copyButton !== null) {
    const status = document.getElementById("copy-status");
    const idleIcon = copyButton.querySelector<SVGElement>("[data-icon='idle']");
    const doneIcon = copyButton.querySelector<SVGElement>("[data-icon='done']");
    let resetTimer: number | undefined;

    // The icon swaps rather than the button's label, so the button keeps its width; the wording goes to
    // the status line beside the step title, which is also what a screen reader announces.
    const showState = (message: string, copied: boolean) => {
        idleIcon?.classList.toggle("hidden", copied);
        doneIcon?.classList.toggle("hidden", copied === false);
        if (status !== null) {
            status.textContent = message;
        }
    };

    const flash = (message: string, copied: boolean) => {
        showState(message, copied);
        window.clearTimeout(resetTimer);
        resetTimer = window.setTimeout(() => showState("", false), RESET_AFTER_MS);
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

    copyButton.addEventListener("click", () => {
        if (navigator.clipboard?.writeText === undefined) {
            offerManualCopy();
            return;
        }

        navigator.clipboard.writeText(command.textContent ?? "").then(() => flash("Copied", true), offerManualCopy);
    });
}
