import setupStep = require("viewmodels/wizard/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/wizard/claimDomainCommand");
import nodeInfo = require("models/wizard/nodeInfo");
import ipEntry = require("models/wizard/ipEntry");
import loadAgreementCommand = require("commands/wizard/loadAgreementCommand");
import getIpsInfoCommand = require("commands/wizard/getIpsInfoCommand");
import viewHelpers = require("common/helpers/view/viewHelpers");

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
            domainModel.availableDomains(Object.keys(userInfo.Domains));            
            domainModel.availableRootDomains(userInfo.RootDomains);
            domainModel.availableEmails(userInfo.Emails);

            if (domainModel.availableDomains().length === 1) {
                domainModel.domain(domainModel.availableDomains()[0]);
            }

            if (domainModel.availableRootDomains().length === 1) {
                domainModel.rootDomain(domainModel.availableRootDomains()[0]);
            }

            if (domainModel.availableEmails().length === 1) {
                domainModel.userEmail(domainModel.availableEmails()[0]);
            }
        }
    }

    back() {
        router.navigate("#license");
    }
    
    save() {
        this.spinners.save(true);
        const domainModel = this.model.domain();
      
        viewHelpers.asyncValidationCompleted(domainModel.validationGroup, () => {
            if (this.isValid(domainModel.validationGroup)) {

                // Get the ips info for the selected rootDomain
                new getIpsInfoCommand(domainModel.rootDomain(), this.model.userDomains())
                    .execute()
                    .done((result: Raven.Server.Commercial.UserDomainsWithIps) => {
                        this.model.userDomains(result);
                        
                        $.when<any>(this.claimDomainIfNeeded(), this.loadAgreementIfNeeded())
                            .done(() => {
                                this.tryPopulateNodesInfo();
                                router.navigate("#nodes");
                            })
                            .always(() => this.spinners.save(false));
                    })
                    .fail(() => this.spinners.save(false));
            } else {
                this.spinners.save(false);
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
                    const entry = new nodeInfo(this.model.hostnameIsNotRequired, this.model.mode);
                    entry.nodeTag(info.SubDomain.split(".")[0].toLocaleUpperCase());
                    entry.ips(info.Ips.map(x => ipEntry.forIp(x, true)));
                    return entry;
                });
                
                if (nodes.length > 0) {
                    this.model.domain().reusingConfiguration(true);
                    this.model.nodes(nodes);
                }
            }
        }
    }
    
    private loadAgreementIfNeeded(): JQueryPromise<void | string> {
        if (this.model.agreementUrl()) {
            return $.when<void>();
        }
        
        return new loadAgreementCommand(this.model.domain().userEmail())
            .execute()
            .done(url => {
                this.model.agreementUrl(url);
            });
    }
    
    private claimDomainIfNeeded(): JQueryPromise<void> {
        const domainModel = this.model.domain();
        
        if (_.includes(domainModel.availableDomains(), domainModel.domain())) {
            // no need to claim it
            return $.when<void>();
        }

        const domainToClaim = domainModel.domain();
        return new claimDomainCommand(domainToClaim, this.model.license().toDto())
            .execute()
            .done(() => {
                this.model.userDomains().Domains[domainToClaim] = [];
                domainModel.availableDomains.push(domainToClaim);
            });
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
