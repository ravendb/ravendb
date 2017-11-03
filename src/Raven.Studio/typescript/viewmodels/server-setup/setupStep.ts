/// <reference path="../../../typings/tsd.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import serverSetup = require("models/setup/serverSetup");

abstract class setupStep extends viewModelBase {
   protected model = serverSetup.default;
   
   //TODO:  create method to validate preconditions of each configurationo step, like (mode is filled in)
   
   
}

export = setupStep;
