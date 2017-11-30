/// <reference path="../../../../typings/tsd.d.ts" />

class serverTime {
   
    static default = new serverTime();
    
    serverTimeDifference = ko.observable<number>();
    
    calcTimeDifference(serverDate: string) {
        const now = moment().utc();
        const dateParam = moment(serverDate).utc();
        
        this.serverTimeDifference(now.diff(dateParam));   
    }

    getAdjustedTime(date: string): moment.Moment {     
      return moment(date).utc().add(moment.duration(this.serverTimeDifference()));   
    }
}

export = serverTime;
