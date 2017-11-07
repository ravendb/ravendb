/// <reference path="../../../typings/tsd.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import serverSetup = require("models/setup/serverSetup");

abstract class setupStep extends viewModelBase {
   protected model = serverSetup.default;
   
}

export = setupStep;
