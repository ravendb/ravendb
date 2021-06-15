/// <reference path="../../typings/tsd.d.ts" />

class browserUtils {

    static isBrowserSupported(): boolean {
        const isChrome = /Chrome/.test(navigator.userAgent) && /Google Inc/.test(navigator.vendor);
        const isFirefox = navigator.userAgent.toLowerCase().indexOf('firefox') > -1;
        
        return isChrome || isFirefox;
    }
} 

export = browserUtils;
