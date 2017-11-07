/// <reference path="../../../typings/tsd.d.ts"/>

class ipEntry {
    
   ip = ko.observable<string>();
   validationGroup: KnockoutValidationGroup;
   
   constructor() {
       this.ip.extend({
           required: true
       });
       
       this.validationGroup = ko.validatedObservable({
           ip: this.ip
       });
   }
}

export = ipEntry;
