import "./expired.css";
import quillLogo from "@/components/brand/quill-logo.svg?raw";

// Inline svg rather than an <img>, so the logo keeps following the theme via currentColor.
document.getElementById("logo")?.insertAdjacentHTML("afterbegin", quillLogo);

const command = document.getElementById("command");
const copyButton = document.getElementById("copy");

if (command !== null && copyButton !== null) {
    let resetTimer: number | undefined;
    const flash = (label: string) => {
        copyButton.textContent = label;
        window.clearTimeout(resetTimer);
        resetTimer = window.setTimeout(() => {
            copyButton.textContent = "Copy";
        }, 2000);
    };

    copyButton.addEventListener("click", () => {
        // Select the command either way, so "Press Ctrl+C" is actionable when the clipboard API
        // is unavailable (the appliance is typically browsed over plain http).
        const range = document.createRange();
        range.selectNodeContents(command);
        const selection = getSelection();
        selection?.removeAllRanges();
        selection?.addRange(range);

        if (navigator.clipboard?.writeText === undefined) {
            flash("Press Ctrl+C");
            return;
        }

        navigator.clipboard.writeText(command.textContent ?? "").then(
            () => {
                selection?.removeAllRanges();
                flash("Copied");
            },
            () => flash("Press Ctrl+C"),
        );
    });
}
