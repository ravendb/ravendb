class svgDownloader {

    static svgHeader = '<?xml version="1.0" standalone="no"?>\n' + 
    '<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">';

    private static convertToData(svgElement: Element) {

        var svgClone = <Element>svgElement.cloneNode(true);

        svgDownloader.fixAttributes(svgClone); 
        var svgContainer = document.createElement('div');
        svgContainer.appendChild(svgClone);

        var targetStyles = svgDownloader.inlineCss(svgClone);

        var s = document.createElement('style');
        s.setAttribute('type', 'text/css');
        s.innerHTML = "<![CDATA[\n" + targetStyles + "\n]]>";

        var defs = document.createElement('defs');
        defs.appendChild(s);

        svgClone.insertBefore(defs, svgClone.firstChild);

        return svgDownloader.svgHeader + "\n" + svgContainer.innerHTML;
    }

    private static cleanup() {
        // clean previous image (in any)
        $("#downloadSvg").remove();
    }

    static b64toBlob(b64Data, contentType, sliceSize?): Blob {
        contentType = contentType || '';
        sliceSize = sliceSize || 512;

        var byteCharacters = atob(b64Data);
        var byteArrays = [];

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
        if (navigator && navigator.msSaveBlob) {
            navigator.msSaveBlob(blob, filename);
        } else {
            var blobUrl = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.id = "downloadSvg";
            (<any>a).download = filename;
            a.href = blobUrl;
            document.body.appendChild(a); // required by firefox
            a.click();
        }
    }

    static downloadSvg(svgElement: Element) {
        svgDownloader.cleanup();
        var textSvgData = svgDownloader.convertToData(svgElement);
        var encodedImage = window.btoa(textSvgData);
        var blob = svgDownloader.b64toBlob(encodedImage, 'image/svg+xml');
        svgDownloader.createLinkAndStartDownload(blob, 'visualization.svg');
    }

    static downloadPng(svgElement: Element) {
        svgDownloader.cleanup();

        var textSvgData = svgDownloader.convertToData(svgElement);
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
            svgDownloader.createLinkAndStartDownload(blob, 'visualization.png');
        }
    }

    private static inlineCss(element: Element) {
        /*
        Original idea was to go through styles but it takes time to generate. Return predefined styles instead.
        Leaving this code here to have an option to easy update inline css required by svg.

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
        }*/

        return '* { box-sizing: border-box; }\n' +
            '.hidden { display: none !important; visibility: hidden !important; }\n' +
            'svg text { font-style: normal; font-variant: normal; font-weight: normal; font-size: 12px; line-height: normal; font-family: Arial; }\n' +
            '.nodeRect { stroke: rgb(119, 119, 119); stroke-width: 1.5px; fill-opacity: 0.4 !important; }\n' +
            '.nodeCheck { stroke-width: 2px; stroke: rgb(0, 0, 0); fill: rgb(255, 255, 255); }\n' +
            '.hidden { display: none; }\n' +
            'g { font-style: normal; font-variant: normal; font-weight: normal; font-size: 10px; line-height: normal; font-family: sans - serif; cursor: pointer; }\n' +
            '.link { fill: none; stroke: rgb(204, 204, 204); stroke-width: 1.5px; }\n' +
            'text { pointer-events: none; text-anchor: middle; }\n' +
            '.link.selected { fill: none; stroke: black; stroke-width: 2.5px; } \n';
    }

    private static fixAttributes(el: Element) {
        var viewBox = el.getAttribute('viewBox').split(/\s+|,/);
        svgDownloader.removeAttributes(el);
        el.setAttribute("version", "1.1");

        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns', 'http://www.w3.org/2000/svg');
        el.setAttributeNS('http://www.w3.org/2000/xmlns/', 'xmlns:xlink', 'http://www.w3.org/1999/xlink');

        el.setAttribute("width", viewBox[2]);
        el.setAttribute("height", viewBox[3]);
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
