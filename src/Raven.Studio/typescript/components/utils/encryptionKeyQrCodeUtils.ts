import { QRCode } from "qrcodejs";

const qrLogo = require("Content/img/qr_logo.png");

function renderQrCode(container: HTMLElement, text: string, size: number) {
    return new QRCode(container, {
        text,
        width: size,
        height: size,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.Q,
    });
}

function getQrCodeHtml(text: string): string {
    const container = document.createElement("div");
    renderQrCode(container, text, 256);

    const canvas = container.querySelector("canvas");
    return canvas ? `<img src="${canvas.toDataURL()}" />` : container.innerHTML;
}

function printEncryptionKey(keyText: string, fileName: string, qrCodeHtml: string) {
    const text = keyText.replace(/\r\n/g, "<br/>");

    const html = `
        <html>
            <head>
                <title>${fileName}</title>
                <style>
                    body {
                        text-align: center;
                        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                    }
                    #encryption_qrcode {
                        position: relative;
                        display: inline-block;
                    }

                    h4 {
                        font-weight: normal;
                    }
                
                    .qr_logo {
                        position: absolute;
                        left: 50%;
                        top: 50%;
                        -moz-transform: translateX(-50%) translateY(-50%);
                        -webkit-transform: translateX(-50%) translateY(-50%);
                        -o-transform: translateX(-50%) translateY(-50%);
                        -ms-transform: translateX(-50%) translateY(-50%);
                        transform: translateX(-50%) translateY(-50%);
                    }
                </style>
            </head>
            <body>
                <h4>${text}</h4>
                <br />
                <div id="encryption_qrcode">
                    <img class="qr_logo" src=${qrLogo}>
                    ${qrCodeHtml}
                </div>
            </body>                
        </html>
    `;

    const printWindow = window.open();
    printWindow.document.write(html);
    printWindow.document.close();

    printWindow.focus();
    setTimeout(() => {
        printWindow.print();
        printWindow.close();
    }, 50);
}

export const encryptionKeyQrCodeUtils = {
    qrLogo,
    renderQrCode,
    getQrCodeHtml,
    printEncryptionKey,
};
