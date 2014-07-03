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

        if ("WebSocket" in window || "EventSource" in window) {
            //Show the app by setting the root view model for our application with a transition.
            app.setRoot('viewmodels/shell');
        } else {
            //The browser doesn't support nor WebSocket nor EventSource. IE 9, Firefox 6, Chrome 6 and below.
            app.showMessage("Your browser isn't supported. Please use a modern browser!", ":-(", []);
            NProgress.done();
        }
    });
});