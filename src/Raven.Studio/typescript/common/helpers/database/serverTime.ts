/// <reference path="../../../../typings/tsd.d.ts" />

class serverTime {
   
    static default = new serverTime();
    
    serverTimeDifference = ko.observable<number>();
    
    calcTimeDifference(serverDate: string) {
        const now = moment.utc();
        this.serverTimeDifference(now.diff(moment.utc(serverDate)));   
    }

    getAdjustedTime(date: moment.Moment): moment.Moment {    
      return date.add(moment.duration(this.serverTimeDifference())); 
    }
}

export = serverTime;
