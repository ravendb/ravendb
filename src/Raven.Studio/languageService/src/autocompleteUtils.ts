import { QuoteType } from "./providers/baseProvider";


export class AutocompleteUtils {

    static quote(input: string, quoteType: QuoteType): string {
        switch (quoteType) {
            case "None":
                return input;
            case "Double":
                return `"${input.replace(/"/g, '\"')}"`;
            case "Single":
                return `'${input.replace(/'/g, "\'")}'`;
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
