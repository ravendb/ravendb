/// <reference path="../../typings/tsd.d.ts" />

class fileImporter {
    
    static readAsText(fileInput: HTMLInputElement, onImport: (data: string, filename: string) => void) {
        return fileImporter.readInternal(fileInput, "readAsText", onImport);
    }
    
    static readAsDataURL(fileInput: HTMLInputElement, onImport: (data: string, filename: string) => void) {
        return fileImporter.readInternal(fileInput, "readAsDataURL", onImport);
    }
    
    static readAsArrayBuffer(fileInput: HTMLInputElement, onImport: (data: any, filename: string) => void) {
        return fileImporter.readInternal(fileInput, "readAsArrayBuffer", onImport);
    }
    
    static readAsBinaryString(fileInput: HTMLInputElement, onImport: (data: string, filename: string) => void) {
        return fileImporter.readInternal(fileInput, "readAsBinaryString", onImport);
    }
    
    private static readInternal(fileInput: HTMLInputElement, mode: keyof FileReader, onImport: (data: string, filename: string) => void): JQueryPromise<void> { 
        const task = $.Deferred<void>();
    
        if (fileInput.files.length === 0) {
            task.reject();
            return task;
        }

        const file = fileInput.files[0];
        const fileName = file.name;
        const reader = new FileReader();
        reader.onload = function() {
            onImport(this.result, fileName);
            task.resolve();
        };

        reader.onerror = function(error: any) {
            alert(error);
            task.reject(error);
        };

        reader[mode](file);
        
        fileInput.value = "";
        return task.promise();
    }
} 

export = fileImporter;
