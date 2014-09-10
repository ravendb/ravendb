# To make an optimized build:

1. Install <a href="https://chocolatey.org">Chocolatey</a>: ```@powershell -NoProfile -ExecutionPolicy unrestricted -Command "iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1'))" && SET PATH=%PATH%;%systemdrive%\chocolatey\bin```
2. Install <a href="http://pvcbuild.com/#getting-started">PVC Build</a>: ```cinst pvc```
3. Open a command prompt Raven.Studio.Html5 directory
4. Run the Raven Studio optimized build script: ```pvc optimized-build```
	
This will create an optimized build output into \Raven.Studio.Html5\optimized-build.

# What's optimized about it?
The optimized build inlines all application scripts and application HTML views into a single file, \App\main.js. This results in faster performance at the cost of higher memory usage.

Additionally, we inline all vendor JS files referenced in index.html, resulting in some 15+ fewer queries to the server on first load.

# How can I edit the optimized build?
Crack open pvcfile.csx. It's C# script. Have at it. Make your changes, hit save, run the build.

TODO:
 
- Combine the CSS into a single concatenated stylesheet.
- Run uglify on the concatenated main.js.
- Figure out how to properly inline Durandal.