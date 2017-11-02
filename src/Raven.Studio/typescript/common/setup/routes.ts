
class routes {

    static get(): Array<DurandalRouteConfiguration> {
        let routes = [
            {
                route: ["", "welcome"],
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/welcome",
                nav: false,
                dynamicHash: () => "#welcome"
            },{
                route: "unsecured",
                title: "RavenDB setup wizard",
                moduleId: "viewmodels/server-setup/unsecured",
                nav: false,
                dynamicHash: () => "#unsecured"
            }
           
        ] as Array<DurandalRouteConfiguration>;

        return routes;
    }

}

export = routes;
