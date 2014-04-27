// Partial type declation for Webshim

interface WebshimStatic {
    polyfill(): void;
}

declare var webshims: WebshimStatic;