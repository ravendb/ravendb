/// <reference path="../../../typings/tsd.d.ts"/>

class routes {

    static get(): Array<DurandalRouteConfiguration> {
        return [
            {
                route: ["", "license"],
                title: "RavenDB EULA",
                moduleId: "viewmodels/eula/license",
                nav: false
            }
        ];
    }

}

export = routes;
