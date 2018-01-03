/// <reference path="../../typings/tsd.d.ts" />
class placeholderUtils {

    static fixPlaceholders(container: JQuery) {
         const isOpera = !!(window as any).opera || navigator.userAgent.indexOf(' OPR/') >= 0;
         
         // Disable for chrome which already supports multiline
         if (! (!!(window as any).chrome && !isOpera)) {
             $("textarea", container).each((idx, el) => {
                let placeholder = $(el).attr('placeholder');
                if (placeholder) {
                    $(el).attr('placeholder', placeholder.replace(/\s\s+/g, ' '));
                }
             });
         }
    }

} 

export = placeholderUtils;
