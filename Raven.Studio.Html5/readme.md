# Introduction

Raven.Studio.Html5 is a replacement of the old Silverlight Studio, built using modern web technologies.

Raven.Studio.Html5 is a single page application (SPA) with no server-side component, communicating only with a Raven server. It uses <a href="http://durandaljs.com">Durandal.js</a> to partition the app into logical modules, <a href="http://getbootstrap.com">Twitter Bootstrap</a> for a consistent UX, <a href="http://knockoutjs.com">KnockoutJS</a> for data-binding, and <a href="http://requirejs.org">RequireJS</a> for loading views and view models on demand.

The application code is written in <a href="http://typescriptlang.org">TypeScript</a>.

## Screenshots
<img src="http://judahhimango.com/images/ravenstudio3.0screen1.png" />
<img src="http://judahhimango.com/images/ravenstudio3.0screen2.png" />
<img src="http://judahhimango.com/images/ravenstudio3.0screen3.png" />

## Code layout
-	<b>Index.html</b> - this is the single page hosting the whole application, pulling in the 3rd party libs and CSS.
-	<b>/App/common</b> - app code shared across multiple views
- <b>/App/models</b> - classes that model the data we get back from Raven, e.g. collection, document, etc.
-	<b>/App/viewmodels</b> - a class for every view. The class contains data and logic for a view. For example, documents.ts is the viewmodel for the documents page. <a href="https://github.com/JudahGabriel/ravendb/blob/Raven.Studio.Html5/Raven.Studio.Html5/App/viewmodels/shell.ts">Shell.ts</a> is of particular interest, as it hosts the application shell, such as the main menu, footer, etc.
-	<b>/App/views</b> - HTML page for every location in the app. For example, documents.html is the view for the documents page.
-	<b>/App/widgets</b> - Contains custom widgets used in the app. Each widget has its own folder containing a view and view model. For example, the documents grid is a custom widget.
-	<b>/Content</b> - images, fonts, CSS/LESS. Of special note is App.less, which contains the styles specific to the app.
-	<b>/Scripts</b> - 3rd party scripts (jquery, Knockout, etc.)
-	<b>/Scripts/typings</b> - TypeScript type definitions for 3rd party libraries.

## Building in Visual Studio 2012
-	For TypeScript compilation, install the <a href="http://go.microsoft.com/fwlink/?LinkID=266563">TypeScript tools</a>. The TypeScript compiler will compile TS files on save.
-	For LESS compilation, install the <a href="http://visualstudiogallery.msdn.microsoft.com/07d54d12-7133-4e15-becb-6f451ea3bea6">Web Essentials</a> plugin.

## Running the app
<b>For development</b>, run a Raven 2.5 server, then run index.html. This requires 2 prereqs:
- The Raven.Server.exe.config must have <code>&lt;add key="Raven/AccessControlAllowOrigin" value="*" /&gt;</code>
- If the server is running somewhere besides http://localhost:8080, change <a href="https://github.com/JudahGabriel/ravendb/blob/Raven.Studio.Html5/Raven.Studio.Html5/App/common/raven.ts#L9">/App/common/Raven.ts <b>baseUrl</b> field</a> accordingly.

<b>For production</b>, Raven.Studio.HTML5 is embedded into the server itself. To run it:
- Change <a href="https://github.com/JudahGabriel/ravendb/blob/Raven.Studio.Html5/Raven.Studio.Html5/App/common/raven.ts#L9">/App/common/Raven.ts <b>baseUrl</b> field</a> to an empty string.
- Build and run the Raven Server, then point your browser to http://[serverurl]/html5/index.html


## Debugging
To debug the TypeScript inside Visual Studio, set a breakpoint as usual and start debugging in Internet Explorer.

To debug TypeScript in the browser, run the app in Google Chrome and debug using the built-in tools (CTRL+Shift+J). Thanks to <a href="http://www.aaron-powell.com/posts/2012-10-03-typescript-source-maps.html">source maps</a>, you'll be able to debug TypeScript code inside Google Chrome, just like you would with normal JavaScript. This will work with any browser that supports source maps.
