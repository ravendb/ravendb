import messagePublisher = require("common/messagePublisher");

class copyToClipboard {
    static async copy(toCopy: string, successMessage?: string) {
        try {
            await navigator.clipboard.writeText(toCopy);
            messagePublisher.reportSuccess(successMessage);
        } catch (err) {
            messagePublisher.reportWarning("Unable to copy to clipboard", err);
        }
    }
}

export = copyToClipboard;
