import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/wizard/claimDomainCommand");
import nodeInfo = require("models/wizard/nodeInfo");
import ipEntry = require("models/wizard/ipEntry");

class domain extends setupStep {

    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt") {
            return $.when({ can: true });
        }

        return $.when({ redirect: "#welcome" });
    }
    
    activate(args: any) {
        super.activate(args);

        const domainModel = this.model.domain();
        const userInfo = this.model.userDomains();
        if (userInfo) {
            domainModel.userEmail(userInfo.Email);
            domainModel.availableDomains(Object.keys(userInfo.Domains));

            if (domainModel.availableDomains().length === 1) {
                domainModel.domain(domainModel.availableDomains()[0]);
            }
        }
    }

    back() {
        router.navigate("#license");
    }
    
    save() {
        const domainModel = this.model.domain();
        this.afterAsyncValidationCompleted(domainModel.validationGroup, () => {
            if (this.isValid(domainModel.validationGroup)) {
                this.claimDomainIfNeeded()
                    .done(() => {
                        this.tryPopulateNodesInfo();
                        router.navigate("#nodes");
                    });
            }
        });
    }
    
    private tryPopulateNodesInfo() {
        this.model.domain().reusingConfiguration(false);
        const domains = this.model.userDomains();
        const chosenDomain = this.model.domain().domain();
        if (domains) {
            const existingDomainInfo = domains.Domains[chosenDomain];
            if (existingDomainInfo) {
                const nodes = existingDomainInfo.map(info => {
                    const entry = new nodeInfo(this.model.hostnameIsNotRequired);
                    entry.nodeTag(info.SubDomain);
                    entry.ips(info.Ips.map(x => ipEntry.forIp(x)));
                    return entry;
                });
                
                if (nodes.length > 0) {
                    this.model.domain().reusingConfiguration(true);
                    this.model.nodes(nodes);
                }
            }
        }
    }
    
    private claimDomainIfNeeded(): JQueryPromise<void> {
        const domainModel = this.model.domain();
        
        if (_.includes(domainModel.availableDomains(), domainModel.domain())) {
            // no need to claim it
            return $.when<void>();
        }

        this.spinners.save(true);
        
        const task = $.Deferred<void>();
        new claimDomainCommand(domainModel.domain(), this.model.license().toDto())
            .execute()
            .done(() => task.resolve())
            .fail(() => task.reject())
            .always(() => this.spinners.save(false));
        
        return task;
    }

    createDomainNameAutocompleter(domainText: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            const key = domainText();
            const availableDomains = this.model.domain().availableDomains();
            
            if (key) {
                return availableDomains.filter(x => x.toLowerCase().includes(key.toLowerCase()));
            } else {
                return availableDomains;
            }           
        });    
    }
}

export = domain;
