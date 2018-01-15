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

    getAdjustedTime(time: moment.Moment): moment.Moment { 
      // Note: Must create a clone, since 'add' mutates original value        
      return time.clone().add(moment.duration(this.serverTimeDifference()));
    }
}

export = serverTime;
