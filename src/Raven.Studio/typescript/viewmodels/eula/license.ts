import viewModelBase = require("viewmodels/viewModelBase");

import getEulaCommand = require("commands/licensing/getEulaCommand");
import acceptEulaCommand = require("commands/licensing/acceptEulaCommand");

class license extends viewModelBase {

    view = require("views/eula/license.html");
    
    spinners = {
        accept: ko.observable<boolean>(false)
    };
    
    eula = ko.observable<string>();
    canAccept = ko.observable<boolean>();
    
    activate(args: any) {
        super.activate(args);
        
        return new getEulaCommand()
            .execute()
            .done((licenseEula: string) => {
                this.eula(licenseEula);    
            });
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        const $pre = $("pre");
        
        $pre.on('scroll', () => {
            if ($pre.scrollTop() >= $pre[0].scrollHeight - $pre.outerHeight() - 30) {
                this.canAccept(true);
            }
        });
    }
    
    accept() {
        this.spinners.accept(true);
        
        new acceptEulaCommand()
            .execute()
            .done(() => {
                window.location.href = "/";
            })
            .fail(() => this.spinners.accept(false));
    }
}

export = license;
