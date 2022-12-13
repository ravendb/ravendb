/// <reference path="../typings/tsd.d.ts" />

require('../wwwroot/Content/css/fonts/icomoon.font');

import { overrideViews } from "./overrides/views";
import { overrideComposition } from "./overrides/composition";
import { overrideSystem } from "./overrides/system";

import "bootstrap/dist/js/bootstrap";
import "jquery-fullscreen-plugin/jquery.fullscreen";
import "bootstrap-select";

import "bootstrap-multiselect";
import "jquery-blockui";

import "bootstrap-duration-picker/src/bootstrap-duration-picker";
import "eonasdan-bootstrap-datetimepicker/src/js/bootstrap-datetimepicker";

import system from "durandal/system";
import app from "durandal/app";

require("prismjs/components/prism-javascript");
require("prismjs/components/prism-csharp");

overrideSystem();
overrideComposition();
overrideViews();

const ko = require("knockout");
require("knockout.validation");
require("knockout-postbox");
require("knockout-delegated-events"); 
const { DirtyFlag } = require("./external/dirtyFlag");
ko.DirtyFlag = DirtyFlag;

system.debug(!(window as any).ravenStudioRelease);

app.title = "Raven.Studio";

const router = require('plugins/router');
router.install();

const bootstrapModal = require("durandalPlugins/bootstrapModal");
bootstrapModal.install();

const dialog = require("plugins/dialog");
dialog.install({});

const pluginWidget = require("plugins/widget");
pluginWidget.install({});

app.start().then(() => {
    if ("WebSocket" in window) {
        if (window.location.pathname.startsWith("/studio")) {
            const shell = require("viewmodels/shell");
            app.setRoot(shell);
        } else if (window.location.pathname.startsWith("/eula")) {
            const eulaShell = require("viewmodels/eulaShell");
            app.setRoot(eulaShell);
        } else {
            const setupShell = require("viewmodels/wizard/setupShell");
            app.setRoot(setupShell);
        }
    } else {
        //The browser doesn't support WebSocket
        app.showBootstrapMessage("Your browser isn't supported. Please use a modern browser!", ":-(", []);
    }
});
