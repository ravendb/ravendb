/// <reference path="../../typings/tsd.d.ts" />
import d3 = require('d3');

class svgDownloader {

    static svgHeader = '<?xml version="1.0" standalone="no"?>\n' + 
    '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">';

    private static convertToData(svgElement: Element, cssInliner: (svg: Element) => string) {

        const svgClone = <Element>svgElement.cloneNode(true);

        svgDownloader.fixAttributes(svgClone); 

        const svgContainer = document.createElement('div');
        svgContainer.appendChild(svgClone);

        const targetStyles = cssInliner(svgClone);

        const s = document.createElement('style');
        s.setAttribute('type', 'text/css');
        s.innerHTML = "<![CDATA[\n" + targetStyles + "\n]]>";

        let defs = d3.select(svgClone).select('defs').node();
        if (!defs) {
            defs = document.createElement('defs');
            svgClone.insertBefore(defs, svgClone.firstChild);
        }
        defs.appendChild(s);

        return svgDownloader.svgHeader + "\n" + svgContainer.innerHTML;
    }

    private static cleanup() {
        // clean previous image (in any)
        $("#downloadSvg").remove();
    }

    static b64toBlob(b64Data: string, contentType:string, sliceSize?:number): Blob {
        contentType = contentType || '';
        sliceSize = sliceSize || 512;

        const byteCharacters = atob(b64Data);
        const byteArrays: Uint8Array[] = [];

        for (let offset = 0; offset < byteCharacters.length; offset += sliceSize) {
            const slice = byteCharacters.slice(offset, offset + sliceSize);

            const byteNumbers = new Array(slice.length);
            for (let i = 0; i < slice.length; i++) {
                byteNumbers[i] = slice.charCodeAt(i);
            }

            const byteArray = new Uint8Array(byteNumbers);

            byteArrays.push(byteArray);
        }

        const blob = new Blob(byteArrays, { type: contentType });
        return blob;
    }

    private static createLinkAndStartDownload(blob: Blob, filename: string) {
        if (navigator && (navigator as any).msSaveBlob) {
            (navigator as any).msSaveBlob(blob, filename);
        } else {
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.id = "downloadSvg";
            (<any>a).download = filename;
            a.href = blobUrl;
            document.body.appendChild(a); // required by firefox
            a.click();
        }
    }

    static downloadSvg(svgElement: Element, filename: string, cssInliner: (svg: Element) => string) {
        svgDownloader.cleanup();
        const textSvgData = svgDownloader.convertToData(svgElement, cssInliner);
        const encodedImage = window.btoa(textSvgData);
        const blob = svgDownloader.b64toBlob(encodedImage, 'image/svg+xml');
        svgDownloader.createLinkAndStartDownload(blob, filename);
    }

    static downloadPng(svgElement: Element, filename: string, cssInliner: (svg: Element) => string) {
        svgDownloader.cleanup();

        const textSvgData = svgDownloader.convertToData(svgElement, cssInliner);
        const image = new Image();
        image.src = 'data:image/svg+xml;base64,' + window.btoa(textSvgData);
        image.onerror = function () {
            alert("Unable to download image!");
        }
        image.onload = function () {
            const canvas = document.createElement('canvas');
            canvas.width = image.width;
            canvas.height = image.height;
            const context = canvas.getContext('2d');
            context.drawImage(image, 0, 0);
            const dataUrlStripped = canvas.toDataURL('image/png').replace(/^data:image\/png;base64,/, "");
            const blob = svgDownloader.b64toBlob(dataUrlStripped, 'image/png');
            svgDownloader.createLinkAndStartDownload(blob, filename);
        }
    }

    // helper method to extract css from element
    public static extractInlineCss(element: Element) {
        
        let targetStyles = "";
        const sheets = document.styleSheets;
        for (let i = 0; i < sheets.length; i++) {
            const rules = (<any>sheets[i]).cssRules;
            for (let j = 0; j < rules.length; j++) {
                const rule = rules[j];
                if (typeof (rule.style) != "undefined") {
                    const elems = element.querySelectorAll(rule.selectorText);
                    if (elems.length > 0) {
                        targetStyles += rule.selectorText + " { " + rule.style.cssText + " }\n";
                    }
                }
            }
        }
        return targetStyles;
    }

    private static fixAttributes(el: Element) {

        const viewBox = el.getAttribute('viewBox');
        
        svgDownloader.removeAttributes(el);
        el.setAttribute("version", "1.1");

        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns', 'http://www.w3.org/2000/svg');
        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns:xlink', 'http://www.w3.org/1999/xlink');

        if (viewBox) {
            const splitedViewBox = viewBox.split(/\s+|,/);
            el.setAttribute("width", splitedViewBox[2]);
            el.setAttribute("height", splitedViewBox[3]);
        }
    }

    private static removeAttributes(el: Element) {
        const attributes = Array.from(el.attributes).map(x => x.name);
        const selection = $(el);
        $.each(attributes, function (i, item) {
            selection.removeAttr(item);
        });
    }
} 

export = svgDownloader
