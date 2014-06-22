import getStatisticsCommand = require("commands/getDatabaseStatsCommand");
import createSampleDataCommand = require("commands/createSampleDataCommand");
import createSampleDataClassCommand = require("commands/createSampleDataClassCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import showSampleDataDialog = require("viewmodels/showSampleDataDialog");

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
 
        new createSampleDataClassCommand(this.activeDatabase())
            .execute()
            .done((results: string) => {
                this.isVisible(true);
                var data = results.replace("\r\n", "");
                app.showDialog(new showSampleDataDialog(data, this.activeDatabase(), false)); //new copyIndexDialog(i.name, this.activeDatabase(), false));
            })
            .always(() => this.isBusy(false));
        
  
    
    }
}


export = createSampleData; 