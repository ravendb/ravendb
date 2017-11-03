/// <reference path="../../../typings/tsd.d.ts"/>

class routes {

    static get(): Array<DurandalRouteConfiguration> {
        let routes = [
            {
                route: ["", "welcome"],
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/welcome",
                nav: false
            },{
                route: "unsecured",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/unsecured",
                nav: false
            },{
                route: "finish",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/finish",
                nav: false
            },{
                route: "license",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/license",
                nav: false
            },{
                route: "domain",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/domain",
                nav: false
            },{
                route: "nodes",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/nodes",
                nav: false
            },{
                route: "agreement",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/agreement",
                nav: false
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes;
    }

}

export = routes;
