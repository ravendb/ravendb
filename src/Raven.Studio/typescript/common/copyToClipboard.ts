import messagePublisher = require("common/messagePublisher");

class copyToClipboard {
    static copy(toCopy: string, successMessage?: string) {
        const dummy = document.createElement("input");
        // Add it to the document
        document.body.appendChild(dummy);
        try {
            dummy.value = toCopy;
            // Select it
            dummy.select();
            // Copy its contents
            const success = document.execCommand("copy");

            if (success) {
                messagePublisher.reportSuccess(successMessage);
            } else {
                messagePublisher.reportWarning("Unable to copy to clipboard");
            }
        } catch (err) {
            messagePublisher.reportWarning("Unable to copy to clipboard", err);
        } finally {
            // Remove it as its not needed anymore
            document.body.removeChild(dummy);
        }
    }
}

export = copyToClipboard;