/// <reference path="../../typings/tsd.d.ts"/>

class inputCursor {
    static setPosition($input: JQuery, position: number) {
        var input = $input[0];
        if (!input) return null;

        if ('createTextRange' in input) {
            var textRange = input['createTextRange']();
            textRange.collapse(true);
            textRange.moveEnd(position);
            textRange.moveStart(position);
            textRange.select();
        } else if ('setSelectionRange' in input) {
            input['setSelectionRange'](position, position);
        }
    }

    static getPosition($input: JQuery): number {
        var input = $input[0];
        if (!input) return null;
        var cursorPosition = 0;
        if ('selectionStart' in input) {
            // Normal browsers
            cursorPosition = input["selectionStart"];
        }
        else {
            // IE
            input.focus();
            var sel = document["selection"].createRange();
            var selLen = document["selection"].createRange().text.length;
            sel.moveStart('character', -input["value"].length);
            cursorPosition = sel.text.length - selLen;
        }
        return cursorPosition;
    
    }
}

export = inputCursor;
