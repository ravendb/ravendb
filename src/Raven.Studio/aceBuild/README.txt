The purpose of the 'aceBuild' project is to provide an option to customize the Ace Editor w/o code duplication.
The overriding files are stored under the 'addons' dir.

To run:
    * Go to 'src\Raven.Studio\aceBuild' folder
    * Verify npm is installed
    * Run 'node Makefile.dryice.js' to build

    The Ace sources and addons will be copied to the build dir 'src\Raven.Studio\aceBuild\build'.
    The output files will be written to 'src\Raven.Studio\wwwroot\Content\ace'.
