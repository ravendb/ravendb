import getStatisticsCommand = require("commands/getDatabaseStatsCommand");
import createSampleDataCommand = require("commands/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/createSampleDataClassCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class createSampleData extends viewModelBase{

    isBusy = ko.observable(false);
   // isEnable = ko.observable(false);
    isEnable = ko.observable(true);
    isVisible =  ko.observable(false);
    classData = ko.observable<string>();
    generateSampleData() {
        this.isBusy(true);
        
        new createSampleDataCommand(this.activeDatabase())
            .execute()
          //  .done(() => this.isEnable(true))
            .always(() => this.isBusy(false));
    }
    showSampleDataClass() {
        var fileDisplayArea = document.getElementById('fileDisplayArea');
        
 
        new createSampleDataClassCommand(this.activeDatabase())
            .execute()
            .done((results: string) => {
              //  this.classData(results);
                this.isVisible(true);
                fileDisplayArea.innerHTML = results.replace("\r\n", "</br>");
            })
            .always(() => this.isBusy(false));
        
  
    
    }
}


export = createSampleData; 