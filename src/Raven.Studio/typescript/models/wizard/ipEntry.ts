/// <reference path="../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");

class ipEntry {
    
   ip = ko.observable<string>();
   validationGroup: KnockoutValidationGroup;
   
   isLocalNetwork: KnockoutComputed<boolean>;

   static runningOnDocker: boolean = false;
   
   constructor() {
       this.ip.extend({
           required: true,
           noPort: true, 
           validation: [
               {
                   validator: (ip: string) => (ipEntry.runningOnDocker && !genUtils.isLocalhostIpAddress(ip)) || !ipEntry.runningOnDocker,  
                   message: "A localhost IP Address is not allowed when running on Docker"
               },
               {
                   validator: (ip: string) => !_.startsWith(ip, "http://") && !_.startsWith(ip, "https://"),
                   message: "Expected valid IP Address/Hostname, not URL"
               }]
       });      
       
       this.validationGroup = ko.validatedObservable({
           ip: this.ip
       });
       
       this.isLocalNetwork = ko.pureComputed(() => {
           const ip = this.ip();
           if (!ip) {
               return false;
           }
           
           return ip === "localhost" || ip === "::1" || ip.startsWith("127.");
       });
   }
   
   static forIp(ip: string) {
       const entry = new ipEntry();
       entry.ip(ip);
       return entry;
   }
}

export = ipEntry;
