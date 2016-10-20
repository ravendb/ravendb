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
            document.execCommand("copy");
        } finally {
            // Remove it as its not needed anymore
            document.body.removeChild(dummy);

            messagePublisher.reportSuccess(successMessage);
        }
    }
}

export = copyToClipboard;