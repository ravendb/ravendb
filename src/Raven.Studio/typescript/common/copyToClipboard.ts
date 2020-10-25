import messagePublisher = require("common/messagePublisher");

class copyToClipboard {

    constructor(private htmlElement: HTMLElement, private textArray: Array<string>) {
        this.htmlElement = htmlElement;
        this.textArray = textArray;
    }
    
    copyText(arrayEntry: number, msg?: string) {
        copyToClipboard.copy(this.textArray[arrayEntry], msg, this.htmlElement)
    }

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
