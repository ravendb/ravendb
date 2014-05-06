import viewmodelBase = require("viewmodels/viewModelBase");
import saveCsvFileCommand = require("commands/saveCsvFileCommand");

class csvImport extends viewmodelBase {
    
    constructor() {
        super();

        

    }

    uploadFile() {
       
    }
    

    attached() {
        var that = this;    
           
        /*document.getElementById('uploadedCsv').addEventListener('change', this.fileChanged, false);*/
        $("#sendFileForm").submit(() => {
            var saveFileFormElement: HTMLFormElement;
            saveFileFormElement = <HTMLFormElement>document.getElementById('sendFileForm');
            var formData = new FormData();
            
            var fileElement = <HTMLInputElement>document.getElementById('uploadedCsv');

            formData.append("csvFile", fileElement.files[0]);

            new saveCsvFileCommand(formData, fileElement.files[0].name, that.activeDatabase()).execute();
        });
    }

    fileChanged() {
        var saveFileFormElement: HTMLFormElement;
        saveFileFormElement = <HTMLFormElement>document.getElementById('sendFileForm');
        saveFileFormElement.submit();
    }

    uploadCsv() {
        
    }

}

export = csvImport; 