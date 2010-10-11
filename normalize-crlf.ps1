foreach ($ext in @("*.cs", "*.js", "*.html", "*.csproject", "*.sln"))  {
	(dir -Recurse -Filter $ext) | foreach { 
		$file = gc $_.FullName
		$file | sc $_.FullName
		}
	
}