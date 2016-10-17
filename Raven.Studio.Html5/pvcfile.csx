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
		"Scripts/Durandal/**/*.js",
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


string ReadAllText(Stream stream)
{
	using (var streamReader = new StreamReader(stream))
	{
		var result = streamReader.ReadToEnd();
		stream.Position = 0;
		return result;
	}
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