/// <reference path="../../typings/tsd.d.ts" />
import d3 = require('d3');

class svgDownloader {

    static svgHeader = '<?xml version="1.0" standalone="no"?>\n' + 
    '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">';

    private static convertToData(svgElement: Element, cssInliner: (svg: Element) => string) {

        var svgClone = <Element>svgElement.cloneNode(true);

        svgDownloader.fixAttributes(svgClone); 

        var svgContainer = document.createElement('div');
        svgContainer.appendChild(svgClone);

        var targetStyles = cssInliner(svgClone);

        var s = document.createElement('style');
        s.setAttribute('type', 'text/css');
        s.innerHTML = "<![CDATA[\n" + targetStyles + "\n]]>";

        var defs = d3.select(svgClone).select('defs').node();
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

        var byteCharacters = atob(b64Data);
        var byteArrays: Uint8Array[] = [];

        for (var offset = 0; offset < byteCharacters.length; offset += sliceSize) {
            var slice = byteCharacters.slice(offset, offset + sliceSize);

            var byteNumbers = new Array(slice.length);
            for (var i = 0; i < slice.length; i++) {
                byteNumbers[i] = slice.charCodeAt(i);
            }

            var byteArray = new Uint8Array(byteNumbers);

            byteArrays.push(byteArray);
        }

        var blob = new Blob(byteArrays, { type: contentType });
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
        var textSvgData = svgDownloader.convertToData(svgElement, cssInliner);
        var encodedImage = window.btoa(textSvgData);
        var blob = svgDownloader.b64toBlob(encodedImage, 'image/svg+xml');
        svgDownloader.createLinkAndStartDownload(blob, filename);
    }

    static downloadPng(svgElement: Element, filename: string, cssInliner: (svg: Element) => string) {
        svgDownloader.cleanup();

        var textSvgData = svgDownloader.convertToData(svgElement, cssInliner);
        var image = new Image();
        image.src = 'data:image/svg+xml;base64,' + window.btoa(textSvgData);
        image.onerror = function () {
            alert("Unable to download image!");
        }
        image.onload = function () {
            var canvas = document.createElement('canvas');
            canvas.width = image.width;
            canvas.height = image.height;
            var context = canvas.getContext('2d');
            context.drawImage(image, 0, 0);
            var dataUrlStripped = canvas.toDataURL('image/png').replace(/^data:image\/png;base64,/, "");
            var blob = svgDownloader.b64toBlob(dataUrlStripped, 'image/png');
            svgDownloader.createLinkAndStartDownload(blob, filename);
        }
    }

    // helper method to extract css from element
    public static extractInlineCss(element: Element) {
        
        var targetStyles = "";
        var sheets = document.styleSheets;
        for (var i = 0; i < sheets.length; i++) {
            var rules = (<any>sheets[i]).cssRules;
            for (var j = 0; j < rules.length; j++) {
                var rule = rules[j];
                if (typeof (rule.style) != "undefined") {
                    var elems = element.querySelectorAll(rule.selectorText);
                    if (elems.length > 0) {
                        targetStyles += rule.selectorText + " { " + rule.style.cssText + " }\n";
                    }
                }
            }
        }
        return targetStyles;
    }

    private static fixAttributes(el: Element) {

        var viewBox = el.getAttribute('viewBox');
        
        svgDownloader.removeAttributes(el);
        el.setAttribute("version", "1.1");

        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns', 'http://www.w3.org/2000/svg');
        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns:xlink', 'http://www.w3.org/1999/xlink');

        if (viewBox) {
            var splitedViewBox = viewBox.split(/\s+|,/);
            el.setAttribute("width", splitedViewBox[2]);
            el.setAttribute("height", splitedViewBox[3]);
        }
    }

    private static removeAttributes(el: Element) {
        var attributes = $.map(el.attributes, function (item) {
            return item.name;
        });
        var selection = $(el);
        $.each(attributes, function (i, item) {
            selection.removeAttr(item);
        });
    }
} 

export = svgDownloader
