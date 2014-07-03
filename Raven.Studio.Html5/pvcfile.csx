pvc.Task("optimized-build", () => {
	var outputDirectory = "optimized-build";
	new DirectoryInfo(outputDirectory).Delete(true);

	pvc.Source(
		"Scripts/typings/**/*.d.ts", 
		"App/**/*.ts",
		"App/views/*.html",
		"App/widgets/**/*.html",
		"App/main.js",
		"fonts/*.woff",
		"Content/**/*",
		"Scripts/**/*.js",
		"Scripts/**/*.css",
		"index.html"
	)

	// Compile all the TypeScript files into JavaScript.
	.Pipe(new PvcTypeScript("--module amd --target ES5"))

	// Convert all the RequireJS modules into named modules. Required for concatenation.
	.Pipe(streams => {
		Console.WriteLine("about to process named modules");
		
		var sourceStreams = streams
			.Where(s => 
				s.StreamName.IndexOf("App\\", StringComparison.InvariantCultureIgnoreCase) >= 0 && 
				!s.StreamName.EndsWith("App\\main.js", StringComparison.InvariantCultureIgnoreCase))
			.ToList();
		var moduleNameGetter = new Func<PvcStream, string>(s => {
			return s.StreamName
				.Replace("App\\", "") // Make it relative to the root: App\commands\someCommand.ts -> command\someCommand.js
				.Replace("\\", "/") // Use forward slash: command\someCommand.js -> command/someCommand.js
				.Replace("virtualTable/viewModel", "virtualTable/viewmodel") // Durandal can't find widgets with a capital letter, apparently.
				.Replace(".js", ""); // Whack off the extension. Can't use Path.GetFileNameWithoutExtension, because it also whacks off the path.
		});
		var namedModulesPlugin = new PvcRequireJSNamedModules(moduleNameGetter);
		
		var result = namedModulesPlugin.Execute(sourceStreams);
		return streams
			.Except(sourceStreams)
			.Concat(result);
	})
		
	// Convert all the HTML views into RequireJS modules.
	.Pipe(streams => {
		Console.WriteLine("views into modules...");
		var inlineHtmlStreamName = "inlineHtmlResults.js";
		var inlineHtmlPlugin = new PvcRequireJsInlineHtml(inlineHtmlStreamName);
		var htmlStreams = streams
			.Where(s => s.StreamName.IndexOf("App\\Views\\", StringComparison.InvariantCultureIgnoreCase) >= 0)
			.ToList();
		var moduleDefinitionStreams = inlineHtmlPlugin.Execute(htmlStreams);
		return streams
			.Except(htmlStreams)
			.Concat(moduleDefinitionStreams)
			.ToList();
	})

	// Concatenate all RequireJS loaded files into a single main.js file.
	// 		- All /App code
	//		- All the inlined HTML views (created above)
	//		- moment.js dependency
	//		- TODO: All the Durandal modules?
	.Pipe(streams => {
		Console.WriteLine("concating to main.js...");
		var mergedFile = "App\\main.js";
		var concatPlugin = new PvcConcat(s => s.StreamName.Contains("main.js"), mergedFile);
		var filesToConcat = streams
			.Where(s => s.StreamName.EndsWith(".js"))
			.Where(s => 
				s.StreamName.IndexOf("App\\", StringComparison.InvariantCultureIgnoreCase) >= 0 || 
				s.StreamName.IndexOf("inlineHtmlResults.js", StringComparison.InvariantCultureIgnoreCase) >= 0 ||
				s.StreamName.IndexOf("moment.js", StringComparison.InvariantCultureIgnoreCase) >= 0)
			.ToList();
		var results = concatPlugin.Execute(filesToConcat);
		return streams
			.Except(filesToConcat)
			.Concat(results);
	})
	
   .Save(outputDirectory);
	
	   
});

/*pvc.Task("build", () => {
	pvc.Source("Raven.Studio.Html5.csproj")
		.Pipe(new PvcMSBuild(
			buildTarget: "Clean;Build",
	        enableParallelism: true
		));
});*/