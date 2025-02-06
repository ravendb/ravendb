import { overrideViews } from "../typescript/overrides/views";
import { overrideComposition } from "../typescript/overrides/composition";
import { overrideSystem } from "../typescript/overrides/system";

overrideSystem();
overrideComposition();
overrideViews();

import system from "durandal/system";
system.debug(true);

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


import { commonInit } from "components/common/shell/setup";

import { fn } from "@storybook/test";
window.jest = { fn }

commonInit();

import studioSettings from "common/settings/studioSettings";
const mockJQueryPromise = () => $().promise();
studioSettings.default.configureLoaders(mockJQueryPromise, mockJQueryPromise, mockJQueryPromise, mockJQueryPromise);

import { StoreDecorator } from "./storeDecorator"

export const decorators = [
    StoreDecorator
]

export const parameters = {
  actions: { }, //TODO: it was regexp
  controls: {
    matchers: {
      color: /(background|color)$/i,
      date: /Date$/,
    },
  },
}

export const globalTypes = {
  theme: {
    name: "Theme",
    description: "Global theme for components",
    defaultValue: "dark",
    toolbar: {
      icon: "paintbrush",
      items: [
        { value: "dark", title: "Default" },
        { value: "classic", title: "Classic" },
        { value: "blue", title: "Blue" },
        { value: "light", title: "Light" },
      ],
    },
  },
  options: {
    storySort: {
      order: [
          "Pages",
          [
              "Documents",
              [
                  "All Documents",
                  "All Revisions",
                  "Revisions Bin",
                  "Path",
                  "Identities",
                  "Compare Exchange",
                  "Conflicts",
              ],
              "Indexes",
              [
                  "Query",
                  "List of Indexes",
                  "Index Performance",
                  "Map-Reduce Virtualizer",
                  "Index Cleanup",
                  "Index Errors",
              ],
              "Tasks",
              ["Backups", "Ongoing Tasks", "Import Data", "Export Data", "Create Sample Data"],
              "Settings",
              [
                  "Database Settings",
                  "Connection Strings",
                  "Conflict Resolution",
                  "Client Configuration",
                  "Studio Configuration",
                  "Document Refresh",
                  "Document Expiration",
                  "Document Compression",
                  "Document Revisions",
                  "Revisions Bin Cleaner",
                  "Revert Revisions",
                  "Data Archival",
                  "Time Series",
                  "Custom Sorters",
                  "Custom Analyzers",
                  "Manage Database Group",
                  "Integrations",
                  "Database Record",
                  "Unused Database IDs",
                  "Tombstones",
              ],
              "Stats",
              ["Stats", "IO Stats", "Storage Report", "Running Queries", "Ongoing Tasks Stats"],
              "Databases",
              "Cluster Dashboard",
              "Manage Server",
              [
                  "Cluster",
                  "Client Configuration",
                  "Studio Configuration",
                  "Server Settings",
                  "Admin JS Console",
                  "Certificates",
                  "Server-Wide Tasks",
                  "Server-Wide Analyzers",
                  "Server-Wide Sorters",
                  "Admin Logs",
                  "Traffic Watch",
                  "Gather Debug Info",
                  "Storage Report",
                  "IO Stats",
                  "Stack Traces",
                  "Running Queries",
                  "Advanced",
              ],
              "About",
          ],
          "Shell",
          "Bits",
      ],
    }
  },
}
