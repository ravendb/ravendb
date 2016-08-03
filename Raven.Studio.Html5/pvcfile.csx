pvc.Task("optimized-build", () => {
	var outputDirectory = "optimized-build";
	if (Directory.Exists(outputDirectory))
		Directory.Delete(outputDirectory, true);

	var typeScriptToolsVersion = GetTypeScriptToolsVersion();
	
	var list = new List<string> {
		"Scripts/typings/**/*.d.ts",
		"App/**/*.ts",
		"App/views/**/*.html",
		"App/widgets/**/*.html",
		"App/main.js",
		"Scripts/**/*.js",
		"Scripts/**/*.css",
		"index.html",
		"version.json"
	};
		
	pvc.Source(list.ToArray())

	// Compile all the TypeScript files into JavaScript.
	.Pipe(new PvcTypeScript(typeScriptToolsVersion, "--module amd --target ES5"))

	// Convert all the RequireJS modules into named modules. 
	// Required for concatenation.
	.Pipe(streams => {
		Console.WriteLine("about to process named modules");
		
		var sourceStreams = streams
			.Where(s => 
				s.StreamName.IndexOf("App\\", StringComparison.InvariantCultureIgnoreCase) >= 0 && 
				!s.StreamName.EndsWith("App\\main.js", StringComparison.InvariantCultureIgnoreCase))
			.ToList();
		var appModuleNameFetcher = new Func<PvcStream, string>(s => {
			return s.StreamName
				.Replace("App\\", "") // Make it relative to the root: App\commands\someCommand.ts -> command\someCommand.js
				.Replace("\\", "/") // Use forward slash: command\someCommand.js -> command/someCommand.js
				.Replace("virtualTable/viewModel", "virtualTable/viewmodel") // Durandal can't find widgets with a capital letter, apparently.
				.Replace(".js", ""); // Whack off the extension. Can't use Path.GetFileNameWithoutExtension, because it also whacks off the path.
		});
		var namedModulesPlugin = new PvcRequireJSNamedModules(appModuleNameFetcher);
		
		var result = namedModulesPlugin.Execute(sourceStreams);
		return streams
			.Except(sourceStreams)
			.Concat(result);
	})

	// Inline Durandal files into main.js right before app initialization.
	.Pipe(streams => {
		Console.WriteLine("Inlining Durandal files...");
		var durandalFiles = streams
			.Where(s => s.StreamName.IndexOf("Scripts\\durandal", StringComparison.InvariantCultureIgnoreCase) >= 0)
			.ToList();

		// First, we must convert them to named modules.
		var durandalFileContents = new PvcRequireJSNamedModules(DurandalModuleNameFetcher)
			.Execute(durandalFiles)
			.Select(s => ReadAllText(s))
			.ToList();
			
		var durandalFileContentsString = string.Join("", durandalFileContents);
		var mainjs = streams.Single(s => s.StreamName.IndexOf("App\\main.js", StringComparison.InvariantCultureIgnoreCase) >= 0);
		var mainjsContents = ReadAllText(mainjs);
		var newMainjsContents = mainjsContents.Replace("// OPTIMIZED BUILD INLINE DURANDAL HERE", durandalFileContentsString);
		var newMainjs = PvcUtil.StringToStream(newMainjsContents, mainjs.StreamName);
		newMainjs.ResetStreamPosition();

		var except = new List<PvcStream> { mainjs };
		var concat = new List<PvcStream> { newMainjs };
		
		return streams
			.Except(durandalFiles) // We're done with the Durandal files; we've now moved them inline.
			.Except(except) // Get rid of the old main.js
			.Concat(concat); // Add the new main.js
	})
		
	// Convert all the HTML views into RequireJS modules.
	.Pipe(streams => {
		Console.WriteLine("views into modules...");
		var inlineHtmlStreamName = "inlineHtmlResults.js";
		
		var viewModuleNameFetcher = new Func<string, string>(s => {
			return "text!views/" + s
				.Replace("App\\views\\", "") // Make it relative to the root
				.Replace("\\", "/"); // Use forward slash: command\someCommand.js -> command/someCommand.js
		});
		
		var inlineHtmlPlugin = new PvcRequireJsInlineHtml(inlineHtmlStreamName, viewModuleNameFetcher);
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
	// We also inline the Durandal scripts manually. While these are loaded via RequireJS at runtime, we need them in index.html before we start executing main.js.
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

		var except = new List<PvcStream> { indexHtmlStream };
		var concat = new List<PvcStream> { indexHtmlWithInlinedScriptsStream };
		
		return streams
			.Except(except) // Get rid of the old index.html
			.Concat(concat); // Add the new index.html
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

string DurandalModuleNameFetcher(PvcStream stream)
{
	var raw = stream.StreamName;
	var fileName = System.IO.Path.GetFileNameWithoutExtension(raw);
	var directory = System.IO.Path.GetDirectoryName(raw);
	var containingFolderIndex = directory.LastIndexOf("\\");
	var containingFolder = directory.Substring(containingFolderIndex + 1);
	var moduleName = containingFolder + "/" + fileName;
	return moduleName;
}

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

string ReadAllText(Stream stream)
{
	using (var streamReader = new StreamReader(stream))
	{
		var result = streamReader.ReadToEnd();
		stream.Position = 0;
		return result;
	}
}

string CreateScriptElementFromStream(Stream stream)
{
	return "<script type='text/javascript'>" + 
		ReadAllText(stream) + 
		"</script>";
}

string GetTypeScriptToolsVersion()
{
	var xmldoc = new System.Xml.XmlDocument();
	xmldoc.Load(@"Raven.Studio.Html5.csproj");

	var ns = new System.Xml.XmlNamespaceManager(xmldoc.NameTable);
	ns.AddNamespace("x", "http://schemas.microsoft.com/developer/msbuild/2003");
	var node = xmldoc.SelectSingleNode("//x:TypeScriptToolsVersion", ns);
	
	if (node == null)
		throw new InvalidOperationException("Could not find TypeScriptToolsVersion in csproj.");
	
	if (string.IsNullOrEmpty(node.InnerText))
		throw new InvalidOperationException("Invalid TypeScriptToolsVersion in csproj file.");
	
	return node.InnerText;
}