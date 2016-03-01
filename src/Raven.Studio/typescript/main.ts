requirejs.config({
    paths: {
        text: '../lib/requirejs-text/text',
        durandal: '../lib/Durandal/js',
        plugins: '../lib/Durandal/js/plugins',
        ace: '../lib/ace/lib/ace'
    },

    map: {
      '*' : {
          "jszip": "../lib/jszip/dist/jszip",
          "jszip-utils": "../lib/jszip-utils/dist/jszip-utils",
          "d3": "../lib/d3/d3",
          "dagre": "../lib/dagre/dist/dagre.core",
          "forge": "../lib/forge/js/forge",
          "moment": "../lib/moment/moment"
      }  
    },

    // 0 disables the timeout completely, default is 7 seconds
    waitSeconds: 30
});

define('jquery', () => jQuery);
define('knockout', () => ko);
/* TODO
define('nvd3', ['d3', 'd3/nv', 'd3/models/timelines', 'd3/models/timelinesChart'], (d3, nv, timelines, chart) => nv);
define('dagre', ['d3', 'd3/dagre'], (d3, dagre) => dagre);
*/
define(["durandal/system", "durandal/app", "durandal/viewLocator", "plugins/dialog", "durandal/composition"], (system, app, viewLocator, dialog, composition) => {
    system.debug(true);
    
    NProgress.configure({ showSpinner: false });

    app.title = 'Raven.Studio';
    dialog.MessageBox.setViewUrl('views/dialog.html');

    app.configurePlugins({
        router: true,
        dialog: true,
        widget: true
    });

    app.start().then(() => {
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
