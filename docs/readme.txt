## HOW TO REFERENCE RAVENDB 4.0 CLIENT

1. Create 'lib' directory in your solution directory.

2. Copy 'Raven.Client' and 'Sparrow' directories from Client\netstandard1.6 to 'lib' directory in your solution directory.

3. Create 'lib' Solution Folder in your solution. To do so right-click root item in Solution Explorer and choose Add -> Add Solution Folder from the context menu.

4. Add 'Raven.Client' project to that 'lib' Solution Folder. To do so right-click 'lib' Solution Folder in Solution Explorer and choose Add -> Add Existing Project. Navigate to lib\Raven.Client directory and select Raven.Client.xproj.

5. Add 'lib' to 'projects' property in global.json Solution Item.

```
{
  "projects": [ "src", "test", "lib" ],
  "sdk": {
    "version": "1.0.0-preview2-003131"
  }
}
```

6. Reference 'Raven.Client' in your project.

NOTE: Due to .NETCore tooling issue [aspnet/Tooling/issues/565](https://github.com/aspnet/Tooling/issues/565) you may experience IntelliSense issues and Visual Studio showing errors for every line of code using wrapped assemblies.