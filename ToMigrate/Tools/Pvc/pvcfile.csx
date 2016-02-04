/*pvc.Task("less", () => {
	pvc.Source("less/*.less")
	   .Pipe(new PvcLess())
	   .Save("bin");
});

pvc.Task("msbuild", () => {
	pvc.Source(@"C:\Projects\FluentAutomation\FluentAutomation.sln")
	   .Pipe(new PvcMSBuild("Clean;Build", "Debug"));
});

pvc.Task("default", () => {
	PvcNuGet.NuGetExePath = @"C:\Chocolatey\bin\NuGet.bat";
	PvcNuGet.ServerUrl = "https://www.myget.org/F/pvctest/api/v2/package";
	PvcNuGet.SymbolServerUrl = "https://nuget.symbolsource.org/MyGet/pvctest";
	PvcNuGet.ApiKey = "";

	pvc.Source(@"C:\Users\stirno\Source\Repos\pvc\Pvc.Core\Pvc.Core.csproj")
	   .Pipe(new PvcNuGetPack(createSymbolsPackage: true))
	   .Pipe(new PvcNuGetPush());
});*/