import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class listHostsForCertificateCommand extends commandBase {

    constructor(private certificate: string, private password: string) {
        super();
    }

    execute(): JQueryPromise<Array<string>> {
        const url = endpoints.global.setup.setupHosts; 
        const payload = {
            Certificate: this.certificate,
            Password: this.password
        };

        const task = $.Deferred<Array<string>>();
        
        this.post(url, JSON.stringify(payload), null)
            .done(result => {
                const cns = [];
                cns.push(result.CN);
                if (result.AlternativeNames) {
                    cns.push(...result.AlternativeNames);
                }
                task.resolve(cns);
            })
            .fail((response: JQueryXHR) => {
                if (response.status !== 400) {
                    this.reportError("Failed to fetch CNs from certificate", response.responseText, response.statusText);    
                }
                task.reject(response);
            });
        
        return task;
    }
}

export = listHostsForCertificateCommand;
