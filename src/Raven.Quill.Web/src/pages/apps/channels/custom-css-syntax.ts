import { CssSyntaxError, parse } from "postcss";

/** Ace lints the CSS with its own worker, but only while the editor is mounted - a collapsed section would
 *  save a broken rule unnoticed - so the schema parses the CSS itself. postcss checks structure only, never
 *  property names or values, so nesting, at-rules and anything newer than the parser still pass. */
export function findCssSyntaxError(css: string): string | undefined {
    try {
        parse(css);
        return undefined;
    } catch (error) {
        if (error instanceof CssSyntaxError) {
            return error.line === undefined ? error.reason : `${error.reason} on line ${error.line}`;
        }

        throw error;
    }
}
