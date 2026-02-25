const path = require("path");

const fixedCodepoints = require("../typescript/common/helpers/view/icomoonFixedCodepoints.json");

const srcDir = path.resolve(process.cwd(), "wwwroot/Content/css/fonts/icomoon");
const distDir = path.resolve(process.cwd(), "wwwroot/Content/css/fonts/icomoon-svgtofont");

// Dynamic import to support ESM in Node v20
import("svgtofont").then(({ default: svgtofont }) => {
    return svgtofont({
        src: srcDir,
        dist: distDir,
        emptyDist: true,
        excludeFormat: ["eot", "ttf", "svg", "symbol.svg"],
        svgicons2svgfont: {
            normalize: true,
            fontHeight: 1024, // Most svg icons are designed on a 1024x1024 grid, so this ensures that all are scaled correctly
        },
        styleTemplates: path.resolve(__dirname, "svgtofont-templates"),
        fontName: "icomoon",
        classNamePrefix: "icon",
        outSVGReact: false,
        website: null,
        addLigatures: true,
        getIconUnicode: (iconName) => {
            const codePointHex = fixedCodepoints[iconName];
            if (!codePointHex) {
                return undefined;
            }

            const codePoint = parseInt(String(codePointHex).replace(/^0x/i, ""), 16);
            if (Number.isNaN(codePoint)) {
                throw new Error(`Invalid fixed code point for icon '${iconName}': '${codePointHex}'`);
            }

            return [String.fromCodePoint(codePoint)];
        },
        css: {
            templateVars: {
                baseSelector: ".icon",
            },
            hasTimestamp: false,
        },
    });
}).then(() => {
    console.log("Icons font generated");
});
