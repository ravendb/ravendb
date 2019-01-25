/// <reference path="../typings/tsd.d.ts" />

requirejs.config({
    paths: {
        text: "../lib/requirejs-text/text",
        durandal: "../lib/Durandal/js",
        plugins: "../lib/Durandal/js/plugins",
        ace: "../Content/ace",
        forge: "../lib/forge/js"
    },

    map: {
      '*' : {
          "jszip": "../lib/jszip/dist/jszip",
          "jszip-utils": "../lib/jszip-utils/dist/jszip-utils",
          "d3": "../Content/custom_d3",
          "cola": "../lib/webcola/WebCola/cola.min",
          "rbush": "../Content/rbush/rbush",
          "toastr": "../lib/toastr/toastr",
          "quickselect": "../Content/rbush/quickselect",
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
    system.debug(!(window as any).ravenStudioRelease);
    
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
            
            if (window.location.pathname.startsWith("/studio")) {
                app.setRoot("viewmodels/shell");
            } else if (window.location.pathname.startsWith("/eula")) {
                app.setRoot("viewmodels/eulaShell");
            } else {
                app.setRoot("viewmodels/wizard/setupShell")
            }
            
            composition.defaultTransitionName = "fadeIn";
        } else {
            //The browser doesn't support WebSocket
            app.showBootstrapMessage("Your browser isn't supported. Please use a modern browser!", ":-(", []);
        }
    });
});
