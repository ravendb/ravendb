﻿import "../wwwroot/Content/css/styles.less"

const ko = require("knockout");
require("knockout.validation");
require("knockout-postbox");
require("knockout-delegated-events");
const { DirtyFlag } = require("external/dirtyFlag");
ko.DirtyFlag = DirtyFlag;

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

export const parameters = {
  actions: { argTypesRegex: "^on[A-Z].*" },
  controls: {
    matchers: {
      color: /(background|color)$/i,
      date: /Date$/,
    },
  },
}
