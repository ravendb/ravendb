/// <reference path="../../../typings/tsd.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import serverSetup = require("models/wizard/serverSetup");
import { exhaustiveStringTuple } from "components/utils/common";
import StudioEnvironment = Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;

abstract class setupStep extends viewModelBase {
   protected model = serverSetup.default;
   
   static readonly environments = exhaustiveStringTuple<StudioEnvironment>()("None", "Development", "Testing", "Production");
}

export = setupStep;
