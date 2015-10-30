foreach ($ext in @(
"*.cs",
"*.sln",
"*.csproj",
"*.vbproj",
"*.fsproj",
"*.dbproj",
"*.ascx",
"*.xaml",
"*.cmd",
"*.ps1",
"*.coffee",
"*.config",
"*.css",
"*.nuspec",
"*.scss",
"*.cshtml",
"*.htm",
"*.html",
"*.js",
"*.ts",
"*.msbuild",
"*.resx",
"*.ruleset",
"*.Stylecop",
"*.targets",
"*.tt",
"*.txt",
"*.vb",
"*.vbhtml",
"*.xml",
"*.xunit",
"*.java"))  {
    (dir -Recurse -Filter $ext) | foreach { 
        $file = gc $_.FullName
        $file | sc $_.FullName
        }
    
}
