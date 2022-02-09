const lodash = require("lodash");
const knockout = require("knockout");
require("knockout-postbox");
const jquery = require("jquery");

global._ = lodash;
global.ko = knockout;
global.$ = jquery;

require("../typescript/test/mocks");


jest.mock('plugins/router', () => ({
    activate: jest.fn()
}));
jest.mock('plugins/dialog', () => ({
}));
jest.mock('durandal/app', () => ({
}));

