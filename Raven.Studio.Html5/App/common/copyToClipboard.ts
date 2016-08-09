import messagePublisher = require("common/messagePublisher");

class copyToClipboard {
    static copy(toCopy: string, successMessage: string) {
        var dummy = document.createElement("input");
        // Add it to the document
        document.body.appendChild(dummy);
        // Set its ID
        dummy.setAttribute("id", "dummy_id");
        // Output the url into it
        var e: any = document.getElementById("dummy_id");
        e.value = toCopy;
        // Select it
        dummy.select();
        // Copy its contents
        document.execCommand("copy");
        // Remove it as its not needed anymore
        document.body.removeChild(dummy);

        messagePublisher.reportSuccess(successMessage);
    }
}

export = copyToClipboard;