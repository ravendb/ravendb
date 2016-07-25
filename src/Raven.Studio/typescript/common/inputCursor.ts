/// <reference path="../../typings/tsd.d.ts"/>

class inputCursor {
    static setPosition($input: JQuery, position: number): void {
        var input = $input[0];
        if (!input) return;

        if ('createTextRange' in input) {
            var textRange = (<any>input)['createTextRange']();
            textRange.collapse(true);
            textRange.moveEnd(position);
            textRange.moveStart(position);
            textRange.select();
        } else if ('setSelectionRange' in input) {
            (<any>input)['setSelectionRange'](position, position);
        }
    }

    static getPosition($input: JQuery): number {
        var input = $input[0];
        if (!input) return null;
        var cursorPosition = 0;
        if ('selectionStart' in input) {
            // Normal browsers
            cursorPosition = (<any>input)["selectionStart"];
        }
        else {
            // IE
            input.focus();
            var sel = (<any>document)["selection"].createRange();
            var selLen = (<any>document)["selection"].createRange().text.length;
            sel.moveStart('character', -(<any>input)["value"].length);
            cursorPosition = sel.text.length - selLen;
        }
        return cursorPosition;
    
    }
}

export = inputCursor;
