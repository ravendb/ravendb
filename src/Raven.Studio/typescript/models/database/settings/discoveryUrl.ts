/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class discoveryUrl {
    discoveryUrlName = ko.observable<string>();
    validationGroup: KnockoutValidationGroup;

    static usingHttps = location.protocol === "https:";
    
    dirtyFlag: () => DirtyFlag;
    
    constructor(urlName: string) {      

        this.discoveryUrlName(urlName);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([           
            this.discoveryUrlName,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    initValidation() {
        this.discoveryUrlName.extend({         
            validUrl: true
        });
        
        if (!discoveryUrl.usingHttps) {
            this.discoveryUrlName.extend({
                validation: [
                    {
                        validator: (val: string) => !val || val.startsWith("http://"),
                        message: "Connecting from unsecured server (http) to secured (https) is not supported."
                    }]
            });
        }

        this.validationGroup = ko.validatedObservable({
            discoveryUrlName: this.discoveryUrlName
        });
    }
}

export = discoveryUrl;
