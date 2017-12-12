/// <reference path="../../../typings/tsd.d.ts"/>

class ipEntry {
    
   ip = ko.observable<string>();
   validationGroup: KnockoutValidationGroup;
   
   constructor() {
       this.ip.extend({
           required: true,
           validIpAddress: true
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
