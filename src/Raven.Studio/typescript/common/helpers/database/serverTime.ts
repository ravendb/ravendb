/// <reference path="../../../../typings/tsd.d.ts" />

import moment = require("moment");

class serverTime {
   
    static default = new serverTime();

    serverTimeDifference = ko.observable<number>();
    startUpTime = ko.observable<moment.Moment>();

    calcTimeDifference(serverDate: string) {
        const now = moment.utc();
        this.serverTimeDifference(moment.utc(serverDate).diff(now));
    }

    setStartUpTime(startUpTime: string) {
        this.startUpTime(moment.utc(startUpTime));
    }

    getAdjustedTime(time: moment.Moment): moment.Moment { 
      // Note: Must create a clone, since 'add' mutates original value        
      return time.clone().add(moment.duration(this.serverTimeDifference()));
    }
}

export = serverTime;
