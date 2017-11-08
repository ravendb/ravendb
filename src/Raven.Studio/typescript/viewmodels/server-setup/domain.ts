import setupStep = require("viewmodels/server-setup/setupStep");
import router = require("plugins/router");
import claimDomainCommand = require("commands/setup/claimDomainCommand");
import nodeInfo = require("models/setup/nodeInfo");
import ipEntry = require("models/setup/ipEntry");


class domain extends setupStep {

    spinners = {
        save: ko.observable<boolean>(false)
    };
    
    canActivate(): JQueryPromise<canActivateResultDto> {
        const mode = this.model.mode();

        if (mode && mode === "LetsEncrypt") {
            return $.when({ can: true });
        }

        return $.when({redirect: "#welcome"});
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

    save() {
        const domainModel = this.model.domain();
        this.afterAsyncValidationCompleted(domainModel.validationGroup, () => {
            if (this.isValid(domainModel.validationGroup)) {
                this.claimDomainIfNeeded()
                    .done(() => {
                        this.tryPopulateNodesInfo();
                        router.navigate("#agreement");
                    });
            }
        });
    }
    
    private tryPopulateNodesInfo() {
        const domains = this.model.userDomains();
        const chosenDomain = this.model.domain().domain();
        if (domains) {
            const existingDomainInfo = domains.Domains[chosenDomain];
            const nodes = existingDomainInfo.map(info => {
                const entry = new nodeInfo();
                entry.nodeTag(info.SubDomain);
                entry.ips(info.Ips.map(x => ipEntry.forIp(x)));
                return entry;
            });
            
            this.model.nodes(nodes);
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

}

export = domain;
