/// <reference path="../../../../typings/tsd.d.ts" />

class serverTime {
   
    static default = new serverTime();

    serverTimeDifference = ko.observable<number>();
    startUpTime = ko.observable<moment.Moment>();
    
    calcTimeDifference(serverDate: string) {
        const now = moment.utc();
        this.serverTimeDifference(now.diff(moment.utc(serverDate)));   
    }

    setStartUpTime(startUpTime: string) {
        this.startUpTime(moment.utc(startUpTime));
    }

    getAdjustedTime(date: moment.Moment): moment.Moment {    
      return date.add(moment.duration(this.serverTimeDifference())); 
    }
}

export = serverTime;
