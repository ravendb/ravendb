import "../wwwroot/Content/css/styles.less"

import { overrideViews } from "../typescript/overrides/views";
import { overrideComposition } from "../typescript/overrides/composition";
import { overrideSystem } from "../typescript/overrides/system";

overrideSystem();
overrideComposition();
overrideViews();

require('../wwwroot/Content/css/fonts/icomoon.font');

const ko = require("knockout");
require("knockout.validation");
import "knockout-postbox";
require("knockout-delegated-events");
const { DirtyFlag } = require("external/dirtyFlag");
ko.DirtyFlag = DirtyFlag;

import extensions from "common/extensions";

extensions.install();

import "bootstrap/dist/js/bootstrap";
import "jquery-fullscreen-plugin/jquery.fullscreen";
import "bootstrap-select";

import "bootstrap-multiselect";
import "jquery-blockui";

import "bootstrap-duration-picker/src/bootstrap-duration-picker";
import "eonasdan-bootstrap-datetimepicker/src/js/bootstrap-datetimepicker";

import bootstrapModal from "durandalPlugins/bootstrapModal";
bootstrapModal.install();

import dialog from "plugins/dialog";
dialog.install({});

import pluginWidget from "plugins/widget";
pluginWidget.install({});


import { ModuleMocker } from 'jest-mock';
window.jest = new ModuleMocker(window);


export const decorators = [
    (Story) => {
        jest.resetAllMocks();

        return (
            <div>
                {Story()}
            </div>
        )
    }
]

export const parameters = {
  actions: { argTypesRegex: "^on[A-Z].*" },
  controls: {
    matchers: {
      color: /(background|color)$/i,
      date: /Date$/,
    },
  },
}
