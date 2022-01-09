import { QuoteType } from "./providers/baseProvider";


export class QuoteUtils {

    static quote(input: string, preferredQuoteType: QuoteType): string {
        switch (preferredQuoteType) {
            case "None":
                if (input.includes('"')) {
                    return QuoteUtils.quote(input, "Double");
                }
                if (input.includes("'")) {
                    return QuoteUtils.quote(input, "Single");
                }
                return input;
            case "Double":
                return `"${input.replace(/"/g, '\\"')}"`;
            case "Single":
                return `'${input.replace(/'/g, "\\'")}'`;
        }
    }
    
    static unquote(input: string): string {
        if (!input) {
            return input;
        }

        if (input.startsWith("'") && input.endsWith("'")) {
            const unquoted = input.substring(1, input.length - 1);
            return unquoted.replace(/\\'/g, "'");
        }

        if (input.startsWith('"') && input.endsWith('"')) {
            const unquoted = input.substring(1, input.length - 1);
            return unquoted.replace(/\\"/g, '"');
        }

        return input;
    }

}
