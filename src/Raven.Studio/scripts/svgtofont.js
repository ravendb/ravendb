const svgtofont = require("svgtofont").default;
const path = require("path");

const fixedCodepoints = require("../typescript/common/helpers/view/icomoonFixedCodepoints.json");

const srcDir = path.resolve(process.cwd(), "wwwroot/Content/css/fonts/icomoon");
const distDir = path.resolve(process.cwd(), "wwwroot/Content/css/fonts/icomoon-svgtofont");

svgtofont({
    src: srcDir,
    dist: distDir,
    emptyDist: true,
    excludeFormat: ["eot", "ttf", "svg", "symbol.svg"],
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
}).then(() => {
    console.log("Icons font generated");
});
