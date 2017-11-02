/// <reference path="../../../typings/tsd.d.ts"/>

type configurationMode = "Unsecured" | "Secured" | "LetsEncrypt"; //tODO use enum SetupMode

class serverSetup {
   static default = new serverSetup();
   
   mode = ko.observable<configurationMode>();
   
   
}

export = serverSetup;
