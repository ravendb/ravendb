import durandalRouter = require("plugins/router");

class status {

	displayName = "status";
	router = null;

	constructor() {

		this.router = durandalRouter.createChildRouter()
			//.makeRelative({ moduleId: 'viewmodels/status', fromParent: true })
            .map([
                { route: 'status', moduleId: 'viewmodels/statistics', title: 'Stats', type: 'intro', nav: false },
				{ route: 'status/statistics',	moduleId: 'viewmodels/statistics',	title: 'Stats',	type: 'intro', nav: true },
				{ route: 'status/userInfo',	moduleId: 'viewmodels/userInfo',	title: 'User Info', type: 'intro', nav: true }
			])
			.buildNavigationModel();
    }

	activate(args) { 
		//this.router.navigate("status/statistics");
    }

    canDeactivate() {
        return true; 
    } 
}

export = status;    