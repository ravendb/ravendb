// Partial type declation for Webshim

interface WebshimStatic {
    polyfill(string?): void;
    setOptions(string, any);
}

declare var webshims: WebshimStatic;