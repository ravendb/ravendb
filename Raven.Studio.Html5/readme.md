# Introduction

Raven.Studio.Html5 is a replacement of the old Silverlight Studio, built using modern web technologies.

Raven.Studio.Html5 is a single page application (SPA) with no server-side component, communicating only with a Raven server. It uses <a href="http://durandaljs.com">Durandal.js</a> to partition the app into logical modules, <a href="http://getbootstrap.com">Twitter Bootstrap</a> for a consistent UX, <a href="http://knockoutjs.com">KnockoutJS</a> for data-binding, and <a href="http://requirejs.org">RequireJS</a> for loading modules on demand.

The application code is written in <a href="http://typescriptlang.org">TypeScript</a>. TypeScipt 1.0 is required to compile Html5 studio.

## Code layout
-	<b>Index.html</b> - this is the single page hosting the app.
-	<b>/App/common</b> - app code shared across multiple views
-   <b>/App/commands</b> - all communication with the server is done via a command class. Each command should derive from commandBase, which provides support for things like HTTP GET, POST, etc. as well as error handling.
-   <b>/App/models</b> - classes that model the data we get back from Raven, e.g. collection, document, etc.
-	<b>/App/viewmodels</b> - a class for every view. The class contains data and logic for a view. For example, documents.ts is the viewmodel for the documents page. <a href="https://github.com/JudahGabriel/ravendb/blob/Raven.Studio.Html5/Raven.Studio.Html5/App/viewmodels/shell.ts">Shell.ts</a> is of particular interest, as it hosts the application shell, such as the main menu, footer, etc.
-	<b>/App/views</b> - HTML page for every location in the app. For example, documents.html is the view for the documents page.
-	<b>/App/widgets</b> - Contains custom widgets used in the app. Each widget has its own folder containing a view and view model. For example, the documents grid is a custom widget.
-	<b>/Content</b> - images, fonts, CSS/LESS. Of special note is App.less, which contains the styles specific to the app.
-	<b>/Scripts</b> - Vendor scripts (jquery, Knockout, etc.)
-	<b>/Scripts/typings</b> - TypeScript type definitions for 3rd party libraries.

## Running the app
Build and run the Raven.Server project. Launch a web browser to localhost:8080, and the Studio will appear.

## Debugging
To debug the TypeScript inside Visual Studio, set a breakpoint as usual and start debugging in Internet Explorer.

To debug TypeScript in the browser, run the app in a browser that supports source maps, such as Google Chrome. Debug using the built-in tools (CTRL+Shift+J in Chrome). Thanks to <a href="http://www.aaron-powell.com/posts/2012-10-03-typescript-source-maps.html">source maps</a>, you'll be able to debug TypeScript code inside Google Chrome, just like you would with normal JavaScript.
