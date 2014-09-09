pvc.Task("optimized-build", () => {
	var outputDirectory = "optimized-build";
	if (Directory.Exists(outputDirectory))
		Directory.Delete(outputDirectory, true);

	pvc.Source(
		"Scripts/typings/**/*.d.ts", 
		"App/**/*.ts",
		"App/views/**/*.html",
		"App/widgets/**/*.html",
		"App/main.js",
		"fonts/*.woff",
		"Content/**/*",
		"Scripts/**/*.js",
		"Scripts/**/*.css",
		"index.html",
		"version.json"
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

	// Inline all the vendor scripts into index.html
	// These are all the scripts inside index.html's "BEGIN VENDOR SCRIPTS"/"END VENDOR SCRIPTS" blocks.
	.Pipe(streams => {
		Console.WriteLine("Inlining vendor scripts...");
		var indexHtmlStream = streams.Single(s => s.StreamName == "index.html");
		var indexHtmlLines = default(List<string>);
		using (var reader = new StreamReader(indexHtmlStream))
		{
			indexHtmlLines = ReadLines(indexHtmlStream)
				.Select(l => l.Trim())
				.ToList();
		}
 
		var vendorScriptLines = indexHtmlLines
			.SkipWhile(l => l != "<!-- BEGIN VENDOR SCRIPTS -->") // Skip down to the vendor script block.
			.Skip(1) // Skip the vendor script begin block.
			.TakeWhile(l => l != "<!-- END VENDOR SCRIPTS -->"); // Read up until the end of the vendor script block.
		var vendorScriptFileNames = vendorScriptLines
			.Select(l => ReadJsFileNameFromScriptElement(l))
			.Select(l => l.Replace("/", "\\")) // Replace forward slash with back slash. Needed for comparison with stream names.
			.ToList();

		Console.WriteLine("Found {0} vendor scripts. Inlining...", vendorScriptFileNames.Count);
		
		var vendorScriptStreams = vendorScriptFileNames
			.Select(f => streams.Single(s => s.StreamName == f));
		var vendorScriptBlocks = vendorScriptStreams
			.Select(s => CreateScriptElementFromStream(s));
		var vendorScriptBlockString = string.Join(Environment.NewLine, vendorScriptBlocks.ToArray());
		
		// Create a new index.html with the inlined scripts.
		var vendorScriptLinesString = string.Join(Environment.NewLine, vendorScriptLines);
		var indexHtml = string.Join(Environment.NewLine, indexHtmlLines);
		var indexHtmlWithInlinedScripts = indexHtml.Replace(vendorScriptLinesString, vendorScriptBlockString);
		var indexHtmlWithInlinedScriptsStream = PvcUtil.StringToStream(indexHtmlWithInlinedScripts, "index.html");

		return streams
			.Except(new[] { indexHtmlStream }) // Get rid of the old index.html
			.Concat(new[] { indexHtmlWithInlinedScriptsStream }); // Add the new index.html
	})

	// Concatenate all RequireJS loaded files into a single main.js file.
	// 		- All /App code
	//		- All the inlined HTML views (created above)
	//		- TODO: All the Durandal modules?
	.Pipe(streams => {
		Console.WriteLine("concating to main.js...");
		var mergedFile = "App\\main.js";
		var concatPlugin = new PvcConcat(s => s.StreamName.Contains("main.js"), mergedFile);
		var filesToConcat = streams
			.Where(s => s.StreamName.EndsWith(".js"))
			.Where(s => 
				s.StreamName.IndexOf("App\\", StringComparison.InvariantCultureIgnoreCase) >= 0 || 
				s.StreamName.IndexOf("inlineHtmlResults.js", StringComparison.InvariantCultureIgnoreCase) >= 0)
				.ToList();
		var results = concatPlugin.Execute(filesToConcat);
		return streams
			.Except(filesToConcat)
			.Concat(results);
	})
	
   .Save(outputDirectory);
	
	   
});

IEnumerable<string> ReadLines(Stream stream)
{
    using (var reader = new StreamReader(stream))
    {
        string line;
        while ((line = reader.ReadLine()) != null)
        {
            yield return line;
        }
    }
}

string ReadJsFileNameFromScriptElement(string scriptElement)
{
	var srcIndicator = " src";
	var srcIndex = scriptElement.LastIndexOf(srcIndicator);
	var jsFileCharacters = scriptElement
		.Skip(srcIndex)
		.SkipWhile(c => c != '"') // Read until we get to the opening quote.
		.Skip(1) // Skip the opening quote.
		.TakeWhile(c => c != '"'); // Read until we get to the ending quote.
	return new string(jsFileCharacters.ToArray());
}

string CreateScriptElementFromStream(Stream stream)
{
	using (var streamReader = new StreamReader(stream))
	{
		var scriptContents = streamReader.ReadToEnd();
		stream.Position = 0;
		return "<script type='text/javascript'>" + 
			scriptContents + 
			"</script>";
	}
}

/*pvc.Task("build", () => {
	pvc.Source("Raven.Studio.Html5.csproj")
		.Pipe(new PvcMSBuild(
			buildTarget: "Clean;Build",
	        enableParallelism: true
		));
});*/