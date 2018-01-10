/// <reference path="../../../typings/tsd.d.ts"/>

class routes {

    static get(): Array<DurandalRouteConfiguration> {
        let routes = [
            {
                route: ["", "license"],
                title: "RavenDB EULA",
                moduleId: "viewmodels/eula/license",
                nav: false
            }
        ] as Array<DurandalRouteConfiguration>;

        return routes;
    }

}

export = routes;
