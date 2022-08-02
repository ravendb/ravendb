/// <reference path="../../../typings/tsd.d.ts"/>

class routes {

    static get(): Array<DurandalRouteConfiguration> {
        const routes: DurandalRouteConfiguration[] = [
            {
                route: ["", "welcome"],
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/welcome",
                nav: false
            },{
                route: ["security"],
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/security",
                nav: false
            },{
                route: "unsecured",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/unsecured",
                nav: false
            },{
                route: "finish",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/finish",
                nav: false
            }, {
                route: "continue",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/continueConfiguration",
                nav: false
            },{
                route: "license",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/license",
                nav: false
            },{
                route: "domain",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/domain",
                nav: false
            },{
                route: "nodes",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/nodes",
                nav: false
            },{
                route: "certificate",
                title: "RavenDB Setup Wizard",
                moduleId: "viewmodels/wizard/certificate",
                nav: false
            }
        ];

        return routes;
    }

}

export = routes;
