/// <reference path="../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class ipEntry {
    
   ip = ko.observable<string>();
   validationGroup: KnockoutValidationGroup;

   static runningOnDocker: boolean = false;
   
   constructor() {
       this.ip.extend({
           required: true,
           validIpAddress: true,
           validation: [
               {
                   validator: (ip: string) => (ipEntry.runningOnDocker && !genUtils.isLocalhostIpAddress(ip)) || !ipEntry.runningOnDocker,  
                   message: "A localhost IP Address is not allowed when running on Docker"
               }]
       });      
       
       this.validationGroup = ko.validatedObservable({
           ip: this.ip
       });
   }
   
   static forIp(ip: string) {
       const entry = new ipEntry();
       entry.ip(ip);
       return entry;
   }
}

export = ipEntry;
