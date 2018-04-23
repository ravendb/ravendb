import messagePublisher = require("common/messagePublisher");

class copyToClipboard {
    static copy(toCopy: string, successMessage?: string, container = document.body) {
        const dummy = document.createElement("textarea");
        // Add it to the document
        container.appendChild(dummy);
        try {
            dummy.value = toCopy;
            // Select it
            dummy.select();
            // Copy its contents
            const success = document.execCommand("copy");

            if (success) {
                if (successMessage) {
                    messagePublisher.reportSuccess(successMessage);
                }
            } else {
                messagePublisher.reportWarning("Unable to copy to clipboard");
            }
        } catch (err) {
            messagePublisher.reportWarning("Unable to copy to clipboard", err);
        } finally {
            // Remove it as its not needed anymore
            container.removeChild(dummy);
        }
    }
}

export = copyToClipboard;
