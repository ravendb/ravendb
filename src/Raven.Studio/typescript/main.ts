/// <reference path="../typings/tsd.d.ts" />

requirejs.config({
    paths: {
        text: "../lib/requirejs-text/text",
        durandal: "../lib/Durandal/js",
        plugins: "../lib/Durandal/js/plugins",
        ace: "../Content/ace"
    },

    map: {
      '*' : {
          "jszip": "../lib/jszip/dist/jszip",
          "jszip-utils": "../lib/jszip-utils/dist/jszip-utils",
          "d3": "../lib/d3/d3",
          "rbush": "../Content/rbush/rbush",
          "toastr": "../lib/toastr/toastr",
          "quickselect": "../Content/rbush/quickselect",
          "forge": "../lib/forge/js/forge",
          "moment": "../lib/moment/moment",
          "plugins/bootstrapModal": "../App/plugins/bootstrapModal"
      }  
    },

    // 0 disables the timeout completely, default is 7 seconds
    waitSeconds: 30
});

define("jquery", () => jQuery);
define("knockout", () => ko); 

define(["durandal/system", "durandal/app", "durandal/viewLocator", "plugins/dialog", "durandal/composition"], (system: any, app: any, viewLocator: any, dialog: any, composition: any) => {
    system.debug(true);
    
    app.title = "Raven.Studio";
    dialog.MessageBox.setViewUrl("views/dialog.html");

    app.configurePlugins({
        router: true,
        dialog: true,
        widget: true,
        bootstrapModal: true
    });

    app.start().then(() => {
        //Replace 'viewmodels' in the moduleId with 'views' to locate the view.
        //Look for partial views in a 'views' folder in the root.
        viewLocator.useConvention();

        if ("WebSocket" in window) {
            //Show the app by setting the root view model for our application with a transition.
            app.setRoot("viewmodels/shell");
            composition.defaultTransitionName = "fadeIn";
        } else {
            //The browser doesn't support WebSocket
            app.showBootstrapMessage("Your browser isn't supported. Please use a modern browser!", ":-(", []);
        }
    });
});
