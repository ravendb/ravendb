# To make an optimized build:

- Install [Chocolatey](https://chocolatey.org): ```@powershell -NoProfile -ExecutionPolicy unrestricted -Command "iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1'))" && SET PATH=%PATH%;%systemdrive%\chocolatey\bin```
- Install <a href="http://pvcbuild.com/#getting-started">PVC Build</a>: ```cinst pvc```
- Open a command prompt Raven.Studio.Html5 directory
- Run the Raven Studio optimized build script: ```pvc optimized-build```
	
This will create an optimized build output into \Raven.Studio.Html5\optimized-build.

# What's optimized about it?
The optimized build inlines all application scripts and HTML views into a single file, \App\main.js. This results in faster performance at the cost of higher memory usage.

TODO: 
 - Combine the CSS into a single concatenated stylesheet.
 - Concatenate the 3rd party libs into a single /vendor scripts file.
 - Run uglify on the concatenated main.js.
