requirejs.config({
    paths: {
        text: '../Scripts/text',
        durandal: '../Scripts/durandal',
        plugins: '../Scripts/durandal/plugins',
        transitions: '../Scripts/durandal/transitions',
        ace: '../Scripts/ace',
        moment: '../Scripts/moment',
        'd3': '../Scripts/d3',
        forge: '../Scripts/forge',
        jszip: '../Scripts/jszip'
    },
    // 0 disables the timeout completely, default is 7 seconds
    waitSeconds: 30
});

define('jquery', function() { return jQuery; });
define('knockout', ko);
define('nvd3', ['d3/d3', 'd3/nv', 'd3/models/timelines', 'd3/models/timelinesChart'], function (d3, nv, timelines, chart) { return nv; });
define('dagre', ['d3/d3', 'd3/dagre'], function (d3, dagre) { return dagre; });

// Do not remove the below comment, as it's used by the optimized build to inline Durandal scripts.
// OPTIMIZED BUILD INLINE DURANDAL HERE

define(["durandal/system", "durandal/app", "durandal/viewLocator", "plugins/dialog", "durandal/composition"], function (system, app, viewLocator, dialog, composition) {
    //system.debug(true);
    
    NProgress.configure({ showSpinner: false });

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
            app.setRoot("viewmodels/shell", "entrance");
            composition.defaultTransitionName = "fadeIn";
        } else {
            //The browser doesn't support nor WebSocket nor EventSource. IE 9, Firefox 6, Chrome 6 and below.
            app.showMessage("Your browser isn't supported. Please use a modern browser!", ":-(", []);
            NProgress.done();
        }
    });
});
