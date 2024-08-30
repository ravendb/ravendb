const lodash = require("lodash");
const knockout = require("knockout");
require("knockout-postbox");
const jquery = require("jquery");

global._ = lodash;
global.ko = knockout;
global.$ = jquery;
global.jQuery = jquery;

require("bootstrap/dist/js/bootstrap");

require("../typescript/test/mocks");

const customHooks = require("../typescript/components/hooks/hooksForAutoMock.json").hooks;

customHooks.forEach(hook => {
    jest.mock("hooks/" + hook);
});

jest.mock("../typescript/common/eventsCollector");
jest.mock("../typescript/common/bindingHelpers/aceEditorBindingHandler");

jest.mock("../typescript/common/versionProvider");

jest.mock('plugins/router', () => ({
    activate: jest.fn(),
    navigate: jest.fn()
}));
jest.mock('plugins/dialog', () => ({
}));
jest.mock('durandal/app', () => ({
}));

jest.mock("react-dnd", () => ({ 
    DndProvider: () => null
}));
jest.mock("react-dnd-html5-backend", () => ({ 
    HTML5Backend: () => null
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
