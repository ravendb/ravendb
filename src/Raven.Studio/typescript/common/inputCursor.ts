/// <reference path="../../typings/tsd.d.ts"/>

class inputCursor {
    static setPosition($input: JQuery, position: number): void {
        const input = $input[0];
        if (!input) return;

        if ('createTextRange' in input) {
            const textRange = (<any>input)['createTextRange']();
            textRange.collapse(true);
            textRange.moveEnd(position);
            textRange.moveStart(position);
            textRange.select();
        } else if ('setSelectionRange' in input) {
            (<any>input)['setSelectionRange'](position, position);
        }
    }

    static getPosition($input: JQuery): number {
        const input = $input[0];
        if (!input) return null;
        let cursorPosition = 0;
        if ('selectionStart' in input) {
            // Normal browsers
            cursorPosition = (<any>input)["selectionStart"];
        } else {
            // IE
            input.focus();
            const sel = (<any>document)["selection"].createRange();
            const selLen = (<any>document)["selection"].createRange().text.length;
            sel.moveStart('character', -(<any>input)["value"].length);
            cursorPosition = sel.text.length - selLen;
        }
        return cursorPosition;
    
    }
}

export = inputCursor;
