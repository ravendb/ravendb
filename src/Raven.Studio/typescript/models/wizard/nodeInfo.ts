/// <reference path="../../../typings/tsd.d.ts"/>
import ipEntry = require("models/wizard/ipEntry");
import genUtils = require("common/generalUtils");

class nodeInfo {
    
    nodeTag = ko.observable<string>();
    ips = ko.observableArray<ipEntry>([]);
    port = ko.observable<string>();
    tcpPort = ko.observable<string>();
    hostname = ko.observable<string>();
    isLocal: KnockoutComputed<boolean>;
    mode: KnockoutObservable<Raven.Server.Commercial.SetupMode | "Continue">;
    
    externalIpAddress = ko.observable<string>();     
    effectiveIpAddresses: KnockoutComputed<string>;
    externalHttpPort = ko.observable<string>();
    externalTcpPort = ko.observable<string>();
    
    ipsContainHostName: KnockoutComputed<boolean>;
    ipContainBindAll: KnockoutComputed<boolean>; // i.e. 0.0.0.0
  
    advancedSettingsCheckBox = ko.observable<boolean>(false);    
    showAdvancedSettings: KnockoutComputed<boolean>;

    validationGroupForSecured: KnockoutValidationGroup;
    validationGroupForLetsEncrypt: KnockoutValidationGroup;

    private hostnameIsOptional: KnockoutObservable<boolean>;
    
    constructor(hostnameIsOptional: KnockoutObservable<boolean>, mode: KnockoutObservable<Raven.Server.Commercial.SetupMode | "Continue">) {
        this.hostnameIsOptional = hostnameIsOptional;
        this.mode = mode;
        
        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.isLocal = ko.pureComputed(() => {
            return this.nodeTag() === 'A';
        });
        
        this.ips.push(new ipEntry());
        
        this.ipsContainHostName = ko.pureComputed(() => {            
            let hostName = false;
            
            this.ips().forEach(ipItem => {
                if (genUtils.isHostname(ipItem.ip())) {
                    hostName = true;
                }
            });
            
            return hostName;                       
        });

        this.ipsContainHostName.subscribe(val => {
            if (val && this.mode() !== 'Secured') {
                this.advancedSettingsCheckBox(true);
            }
        });
        
        this.ipContainBindAll = ko.pureComputed(() => {
            let hasBindAll = false;
            
            this.ips().forEach(ipItem => {
                if (ipItem.ip() === "0.0.0.0") {
                    hasBindAll = true;
                }
            });
            
            return hasBindAll;
        });
        
        this.ipContainBindAll.subscribe(val => {
            if (val && this.mode() === "LetsEncrypt") {
                this.advancedSettingsCheckBox(true);
            }
        });
        
        this.showAdvancedSettings = ko.pureComputed(() => {
            if (this.ipsContainHostName() && this.mode() !== 'Secured'){
                return true;
            }

            return this.advancedSettingsCheckBox();
        });
        
        this.effectiveIpAddresses = ko.pureComputed(() => {
            const externalIp = this.externalIpAddress();
            if (externalIp && this.showAdvancedSettings()) {
                return externalIp;
            }

            if (this.ips().length) {
                return this.ips().map(ipItem => ipItem.ip()).join(", ");
            }

            return "";
        });
    }

    private initValidation() {
        this.port.extend({
            number: true
        });
        
        this.tcpPort.extend({
            number: true
        });
        
        this.externalHttpPort.extend({
            number: true
        });
        
        this.externalTcpPort.extend({
            number: true
        });
        
        this.externalIpAddress.extend({
            required: {   
                onlyIf: () => this.ipsContainHostName() && !this.externalIpAddress(),                
                message: "This field is required when an address contains Hostname"
            },
            validAddressWithoutPort: true,
            validation: [{
                validator: (val: string) => !genUtils.isHostname(val),
                message: "Please enter IP Address, not Hostname"
            }]
        });
        
        this.hostname.extend({
            validation: [{
                validator: (val: string) => this.hostnameIsOptional() || _.trim(val),
                message: "This field is required"
            }]
        });
        
        this.ips.extend({
            validation: [
                {
                    validator: () => this.ips().length > 0,
                    message: "Please define at least one IP for this node"
                }
            ]
        });

        this.validationGroupForLetsEncrypt= ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port, 
            tcpPort: this.tcpPort,
            ips: this.ips,
            hostname: this.hostname,
            externalIpAddress: this.externalIpAddress,
            externalTcpPort: this.externalTcpPort,
            externalHttpPort: this.externalHttpPort
        });

        this.validationGroupForSecured = ko.validatedObservable({
            nodeTag: this.nodeTag,
            port: this.port,
            tcpPort: this.tcpPort,
            ips: this.ips,
            hostname: this.hostname,
            externalTcpPort: this.externalTcpPort,
            externalHttpPort: this.externalHttpPort
        });
    }

    addIpAddress() {
        this.ips.push(new ipEntry());
    }

    removeIp(ipEntry: ipEntry) {
        this.ips.remove(ipEntry);
    }
    
    getServerUrl() {
        if (!this.hostname()) {
            return null;
        }
        
        let serverUrl = "https://" + this.hostname();
        if (this.port() && this.port() !== "443") {
            serverUrl += ":" + this.port();
        }
        return serverUrl;
    }

    toDto(): Raven.Server.Commercial.SetupInfo.NodeInfo {
        return {
            Addresses: this.ips().map(x => x.ip()),
            Port: this.port() ? parseInt(this.port(), 10) : null,
            PublicServerUrl: this.getServerUrl(),
            ExternalIpAddress: (this.advancedSettingsCheckBox() && this.externalIpAddress()) ? this.externalIpAddress() : null, 
            TcpPort: this.tcpPort() ? parseInt(this.tcpPort(), 10) : null,
            ExternalPort: (this.advancedSettingsCheckBox() && this.externalHttpPort()) ? parseInt(this.externalHttpPort(), 10) : null,
            ExternalTcpPort: (this.advancedSettingsCheckBox() && this.externalTcpPort()) ? parseInt(this.externalTcpPort(), 10) : null
        } as Raven.Server.Commercial.SetupInfo.NodeInfo;
    }
}

export = nodeInfo;
