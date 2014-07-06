requirejs.config({
    paths: {
        text: '../Scripts/text',
        durandal: '../Scripts/durandal',
        plugins: '../Scripts/durandal/plugins',
        transitions: '../Scripts/durandal/transitions',
        ace: '../Scripts/ace',
        moment: '../Scripts/moment',
        'd3': '../Scripts/nvd3',
        forge: '../Scripts/forge'
    }
});

define('jquery', function() { return jQuery; });
define('knockout', ko);
define('nvd3', ['d3/d3', 'd3/nv', 'd3/models/timelines', 'd3/models/timelinesChart'], function(d3, nv, timelines, chart) { return nv; });

define(['durandal/system', 'durandal/app', 'durandal/viewLocator', 'plugins/dialog'], function (system, app, viewLocator, dialog) {
    //>>excludeStart("build", true);
	system.debug(true);
	NProgress.configure({ showSpinner: false });
	//>>excludeEnd("build");

    app.title = 'Raven.Studio';
    dialog.MessageBox.setViewUrl('views/dialog.html');

    app.configurePlugins({
        router: true,
        dialog: true,
        widget: true
    });

    app.start().then(function() {
        //Replace 'viewmodels' in the moduleId with 'views' to locate the view.
        //Look for partial views in a 'views' folder in the root.
        viewLocator.useConvention();

        //Show the app by setting the root view model for our application with a transition.
        app.setRoot('viewmodels/shell');
    });
});
