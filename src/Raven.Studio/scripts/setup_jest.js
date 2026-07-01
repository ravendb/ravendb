/* global jest, $ */

const lodash = require("lodash");
const knockout = require("knockout");
require("knockout-postbox");
const jquery = require("jquery");
const ROP = require("@juggle/resize-observer")
const { hooksForAutoMock } = require("../typescript/components/hooks/hooksForAutoMock");

global._ = lodash;
global.ko = knockout;
global.$ = jquery;
global.jQuery = jquery;

require("bootstrap/dist/js/bootstrap");

require("../typescript/test/mocks");

hooksForAutoMock.forEach(hook => {
    jest.mock("hooks/" + hook);
});

jest.mock("react-markdown", () => {
    const React = require("react");

    function ReactMarkdown({ children }) {
        return React.createElement(React.Fragment, null, children);
    }

    return {
        __esModule: true,
        default: ReactMarkdown,
        Markdown: ReactMarkdown,
    };
});

jest.mock("remark-gfm", () => ({
    __esModule: true,
    default: () => undefined,
}));

jest.mock("../typescript/common/eventsCollector", () => ({
    default: new (require("../typescript/test/mocks/hooks/MockEventsCollector").default)()
}));

jest.mock("../typescript/common/bindingHelpers/aceEditorBindingHandler");

jest.mock("../typescript/common/versionProvider");

jest.mock('plugins/router', () => ({
    activate: jest.fn(),
    navigate: jest.fn(),
    activeInstruction: {
        subscribe: jest.fn()
    },
}));
jest.mock('plugins/dialog', () => ({
}));
jest.mock('durandal/app', () => ({
}));

const ace = require("ace-builds/src-noconflict/ace");
ace.config.set("basePath", "../node_modules/ace-builds/src-noconflict");
window.ace = ace;

window.Worker = class Worker {
    constructor(stringUrl) {
      this.url = stringUrl;
    }

    onmessage = () => null;
    postMessage = () => null;
    terminate = () => null;
}

const studioSettings = require("common/settings/studioSettings");
const mockJQueryPromise = () => $().promise();
studioSettings.default.configureLoaders(mockJQueryPromise, mockJQueryPromise, mockJQueryPromise, mockJQueryPromise);

Storage.prototype.getObject = jest.fn(() => null);

global.define = function() {};

Object.defineProperty(HTMLElement.prototype, "scrollWidth", {
    configurable: true,
    value: 800,
});
Object.defineProperty(HTMLElement.prototype, "offsetWidth", {
    configurable: true,
    value: 800,
});
Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
    configurable: true,
    value: 1000,
});
Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    value: 1000,
});

Object.defineProperty(HTMLElement.prototype, "scrollTo", {
    configurable: true,
    value: jest.fn(),
});

if (!window.ResizeObserver) {
  window.ResizeObserver = ROP.ResizeObserver;
}

if (!window.IntersectionObserver) {
  window.IntersectionObserver = class {
    constructor(callback) {
      this.callback = callback;
    }
    observe() {}
    unobserve() {}
    disconnect() {}
    takeRecords() {
      return [];
    }
  };
}

window.HTMLElement.prototype.getBoundingClientRect = () => ({
    width: 500,
    height: 500,
    bottom: 0,
    left: 0,
    right: 0,
    top: 0,
});
global.structuredClone = (val) => JSON.parse(JSON.stringify(val))

window.HTMLElement.prototype.getBoundingClientRect = () => ({
    width: 500,
    height: 500,
    bottom: 0,
    left: 0,
    right: 0,
    top: 0,
});
