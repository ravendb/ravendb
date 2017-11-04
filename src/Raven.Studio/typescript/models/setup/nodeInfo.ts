/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");

class nodeInfo {
    
    useOwnCertificates: KnockoutObservable<boolean>;
    
    nodeTag = ko.observable<string>(); //TODO: do we need it
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateFileName = ko.observable<string>();
    
    ipInput = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    
    
    private constructor(useOwnCertificates: KnockoutObservable<boolean>) {
        this.useOwnCertificates = useOwnCertificates;
        this.initValidation();
    }

    private initValidation() {
        //TODO: ips
        if (this.useOwnCertificates) {
            this.certificate.extend({
                required: true
            });
            
            this.nodeTag.extend({
                required: true
            })
        }
        
        this.port.extend({
            required: true,
            number: true
        });
        
        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port, 
            certificate: this.certificate,
            certificatePassword: this.certificatePassword
            //TODO: ips, 
        });
    }
    
    addIp() {
        if (this.ipInput()) {
            const entry = new ipEntry();
            entry.ip(this.ipInput());
            this.ips.push(entry);
            
            this.ipInput("");
        }
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }

    fileSelected(fileInput: HTMLInputElement) {
        if (fileInput.files.length === 0) {
            return;
        }

        const fileName = fileInput.value;
        const isFileSelected = fileName ? !!fileName.trim() : false;
        this.certificateFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const dataUrl = reader.result;
            // dataUrl has following format: data:;base64,PD94bW... trim on first comma
            this.certificate(dataUrl.substr(dataUrl.indexOf(",") + 1));
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsDataURL(file);
    }
    
    static empty(ownCertificates: KnockoutObservable<boolean>) {
        return new nodeInfo(ownCertificates);
    }
    
    toDto(): Raven.Server.Commercial.SetupInfo.NodeInfo {
        return {
            Certificate: this.certificate(),
            Password: this.certificatePassword(),
            Ips: this.ips().map(x => x.ip()),
            Port: parseInt(this.port(), 10)
        };
    }
}

export = nodeInfo;
