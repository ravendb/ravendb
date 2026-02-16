
import fixedCodepoints = require("./icomoonFixedCodepoints.json");

class icomoonHelpers {

    /**
     * Fixed codepoints shared with font generation (see `icomoonFixedCodepoints.json`).
     */
    static fixedCodepoints = fixedCodepoints;
    
    static getCodePointForCanvas(iconName: keyof typeof icomoonHelpers.fixedCodepoints): string {
        const codePointHex = icomoonHelpers.fixedCodepoints[iconName];
        if (!codePointHex) {
            console.log("Unable to find code point for: " + iconName);
            return icomoonHelpers.getCodePointForCanvas("placeholder");
        }

        const codePoint = parseInt(codePointHex.replace(/^0x/i, ""), 16);
        if (Number.isNaN(codePoint)) {
            console.log("Invalid code point for: " + iconName + ", value: " + codePointHex);
            return icomoonHelpers.getCodePointForCanvas("placeholder");
        }
        
        return "&#x" + codePoint.toString(16) + ";";
    }
}

export = icomoonHelpers;
