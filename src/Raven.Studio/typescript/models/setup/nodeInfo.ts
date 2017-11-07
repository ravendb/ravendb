/// <reference path="../../../typings/tsd.d.ts"/>

import ipEntry = require("models/setup/ipEntry");
import listHostsForCertificateCommand = require("commands/setup/listHostsForCertificateCommand");

class nodeInfo {
    
    useOwnCertificates: KnockoutObservable<boolean>;
    
    nodeTag = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateFileName = ko.observable<string>();
    
    certificateCNs = ko.observableArray<string>([]);
    
    validationGroup: KnockoutValidationGroup;
    
    private constructor(useOwnCertificates: KnockoutObservable<boolean>) {
        this.useOwnCertificates = useOwnCertificates;
        this.initValidation();
        
        const fetchCNsThrottled = _.debounce(() => this.fetchCNs(), 700);
        
        this.certificate.subscribe(fetchCNsThrottled);
        this.certificatePassword.subscribe(fetchCNsThrottled);
        
        this.ips.push(new ipEntry());
    }
    
    private fetchCNs() {
        new listHostsForCertificateCommand(this.certificate(), this.certificatePassword())
            .execute()
            .done((hosts: Array<string>) => {
               this.certificateCNs(hosts); 
            });
    }

    private initValidation() {
        this.certificate.extend({
            required: {
                onlyIf: () => this.useOwnCertificates()
            }
        });
        
        this.certificate.extend({
            required: {
                onlyIf: () => this.useOwnCertificates()
            }
        });
        
        this.port.extend({
            required: true,
            number: true
        });
        
        this.ips.extend({
            validation: [
                {
                    validator: () => this.ips().length > 0,
                    message: "Please define at least one IP for this node"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port, 
            certificate: this.certificate,
            certificatePassword: this.certificatePassword,
            ips: this.ips
        });
    }

    addIpAddress() {
        this.ips.push(new ipEntry());
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
            Port: parseInt(this.port(), 10),
            ServerUrl: "need to compute it from provided certificate"
        };
    }
}

export = nodeInfo;
