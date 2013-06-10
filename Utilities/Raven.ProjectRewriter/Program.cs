//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Raven.ProjectRewriter
{
	class Program
	{
		static XNamespace xmlns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
		private static Dictionary<string, string> net45Guids;

		private static void Main(string[] args)
		{
			//Generate35(@"Raven.Abstractions\Raven.Abstractions.csproj",
			//    @"Raven.Abstractions\Raven.Abstractions.g.3.5.csproj",
			//    "Raven.Json");
			//Generate35(@"Raven.Client.Lightweight\Raven.Client.Lightweight.csproj",
			//    @"Raven.Client.Lightweight\Raven.Client.Lightweight.g.3.5.csproj",
			//    "Raven.Json",
			//    "Raven.Abstractions");

			/*GenerateSilverlight4(@"Raven.Client.Silverlight\Raven.Client.Silverlight.csproj",
			                     @"Raven.Client.Silverlight\Raven.Client.Silverlight.g.4.csproj");*/

			net45Guids = new Dictionary<string, string>
			{
				{"Raven.Abstractions", "{B903FE56-0230-46FE-9458-AEFFEE294179}"},
				{"Raven.Client.Lightweight", "{E43AA81B-E924-4D7E-8C02-7EF691EBE9EC}"},
				{"Raven.Database", "{FAEBA971-1A36-4D42-8E98-043E617F1FE5}"},
				{"Raven.Client.Embedded", "{ACA1B0BD-3455-4EC4-9388-539EF7CFC945}"},
				{"Raven.Server", "{516EAEEA-D566-4410-BB9F-8354E5611B58}"},
				{"Raven.Tests.Helpers", "{41D3D8AD-9095-47C3-93BE-3023857574AF}"},
			};

			Generate45("Raven.Abstractions");

			Generate45("Raven.Client.Lightweight",
			           "Raven.Abstractions");

			Generate45("Raven.Database",
					   "Raven.Abstractions");

			Generate45("Raven.Client.Embedded",
			           "Raven.Abstractions",
			           "Raven.Client.Lightweight",
			           "Raven.Database");

			Generate45("Raven.Server",
					   "Raven.Abstractions",
					   "Raven.Database");

			Generate45("Raven.Tests.Helpers",
			           "Raven.Abstractions",
			           "Raven.Client.Lightweight",
					   "Raven.Client.Embedded",
					   "Raven.Database",
					   "Raven.Server");

		}

		private static void Generate45(string assemblyName, params string[] references)
		{
			string srcPath = assemblyName + @"\" + assemblyName + ".csproj";
			string destFile= assemblyName + @"\" + assemblyName + ".g.45.csproj";

			var database = XDocument.Load(srcPath);
			foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
			{
				if (element.Value.EndsWith(";") == false)
					element.Value += ";";
				element.Value += "NET45";
			}

			foreach (var element in database.Root.Descendants(xmlns + "ProjectReference").ToArray())
			{
				if (references.Contains(element.Element(xmlns + "Name").Value) == false)
					continue;
				element.Attribute("Include").Value = element.Attribute("Include").Value.Replace(".csproj", ".g.45.csproj");
				element.Element(xmlns + "Project").Value = net45Guids[element.Element(xmlns + "Name").Value];
				element.Element(xmlns + "Name").Value += "-4.5";
			}

			foreach (var element in database.Root.Descendants(xmlns + "ProjectGuid"))
			{
				element.Value = net45Guids[assemblyName];
			}
			foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkVersion"))
			{
				element.Value = "v4.5";
			}
			foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkProfile"))
			{
				element.Value = ""; // Not "Client"
			}
			foreach (var element in database.Root.Descendants(xmlns + "AssemblyName"))
			{
				element.Value += "-4.5";
			}
			foreach (var element in database.Root.Descendants(xmlns + "PropertyGroup"))
			{
				var outputPath = element.Descendants(xmlns + "OutputPath").FirstOrDefault();
				if (outputPath != null)
				{
					outputPath.Value += @"net45\";
				}

				var documentationFile = element.Descendants(xmlns + "DocumentationFile").FirstOrDefault();
				if (documentationFile != null)
				{
					documentationFile.Value = documentationFile.Value.Replace(@"..\build", @"..\build\net45\");
				}
			}

			foreach (var element in database.Root.Descendants(xmlns + "Reference").ToArray())
			{
				if (element.Attribute("Include").Value == "Microsoft.CompilerServices.AsyncTargetingPack.Net4")
				{
					element.Element(xmlns + "HintPath").Value = element.Element(xmlns + "HintPath").Value.Replace(@"net40\Microsoft.CompilerServices.AsyncTargetingPack.Net4", @"net45\Microsoft.CompilerServices.AsyncTargetingPack.Net45");
				}
				else if (element.Attribute("Include").Value == "System.Reactive.Core")
				{
					element.Element(xmlns + "HintPath").Value = element.Element(xmlns + "HintPath").Value.Replace("net40", "net45");
				}
			}

			using (var xmlWriter = XmlWriter.Create(destFile,
													new XmlWriterSettings
													{
														Indent = true
													}))
			{
				database.WriteTo(xmlWriter);
				xmlWriter.Flush();
			}
		}

		private static void GenerateSilverlight4(string srcPath, string destFile, params string[] references)
		{
			var database = XDocument.Load(srcPath);
			foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
			{
				if (element.Value.EndsWith(";") == false)
					element.Value += ";";
				element.Value += "SL_4";
			}

			foreach (var element in database.Root.Descendants(xmlns + "ProjectReference").ToArray())
			{
				if (references.Contains(element.Element(xmlns + "Name").Value) == false)
					continue;
				element.Attribute("Include").Value = element.Attribute("Include").Value.Replace(".csproj", ".g.4.csproj");
				element.Element(xmlns + "Name").Value += "-4";
			}

			foreach (var element in database.Root.Descendants(xmlns + "Reference").ToArray())
			{
				string include = element.Attribute("Include").Value;
				if (include == "AsyncCtpLibrary_Silverlight5")
				{
					element.Attribute("Include").Value = "AsyncCtpLibrary_Silverlight";
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					if (hintPath != null)
						hintPath.Value = hintPath.Value.Replace("Silverlight5", "Silverlight");
				}
				if (include.Contains("Version=5.0.5.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35"))
				{
					element.Attribute("Include").Value = include.Replace("5.0.5.0", "2.0.5.0");
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					if (hintPath != null)
						hintPath.Value = hintPath.Value.Replace(@"Microsoft SDKs\Silverlight\v5.0\", @"Microsoft SDKs\Silverlight\v4.0\");
				}
				if (include.Contains("Version=5.0.5.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35"))
					element.Attribute("Include").Value = include.Replace("5.0.5.0", "2.0.5.0");

				if (include.StartsWith("Microsoft.Threading.Tasks"))
				{
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					hintPath.Value = hintPath.Value.Replace(@"\sl5\", @"\portable-net40+sl4+win8+wp71\");
				}

				if (include.StartsWith("System.Runtime, Version") || include.StartsWith("System.Threading.Tasks"))
				{
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					hintPath.Value = hintPath.Value.Replace(@"\sl5\", @"\sl4\");
				}

				if (include.StartsWith("System.Reactive."))
				{
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					hintPath.Value = hintPath.Value.Replace(@"\SL5\", @"\SL4\");
				}
			}

			foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkVersion"))
			{
				element.Value = "v4.0";
			}
			foreach (var element in database.Root.Descendants(xmlns + "AssemblyName"))
			{
				element.Value += "-4";
			}
			using (var xmlWriter = XmlWriter.Create(destFile,
													new XmlWriterSettings
													{
														Indent = true
													}))
			{
				database.WriteTo(xmlWriter);
				xmlWriter.Flush();
			}
		}

		private static void Generate35(string srcPath, string destFile, params string[] references)
		{
			var database = XDocument.Load(srcPath);
			foreach (var element in database.Root.Descendants(xmlns + "DefineConstants").ToArray())
			{
				if (element.Value.EndsWith(";") == false)
					element.Value += ";";
				element.Value += "NET35";
			}

			foreach (var element in database.Root.Descendants(xmlns + "ProjectReference").ToArray())
			{
				if (references.Contains(element.Element(xmlns + "Name").Value) == false)
					continue;
				element.Attribute("Include").Value = element.Attribute("Include").Value.Replace(".csproj", ".g.3.5.csproj");
				{
					element.Element(xmlns + "Project").Value = "{4C18FC25-0B1E-42E3-A423-3A99F1AC57EE}";
					element.Element(xmlns + "Name").Value += "-3.5";
				}
			}

			foreach (var element in database.Root.Descendants(xmlns + "Reference").ToArray())
			{
				if (element.Attribute("Include").Value == "Microsoft.CSharp")
					element.Remove();
				if (element.Attribute("Include").Value == "AsyncCtpLibrary")
					element.Remove();
				if (element.Attribute("Include").Value == "System.ComponentModel.Composition")
					element.Remove();

				var nugetPakcages = new[] {"Newtonsoft.Json", "NLog"};
				if (nugetPakcages.Any(x => element.Attribute("Include").Value.StartsWith(x)))
				{
					element.Element(xmlns + "HintPath").Value = element.Element(xmlns + "HintPath").Value.Replace("net40", "net35");
				}
			}

			foreach (var element in database.Root.Descendants(xmlns + "DocumentationFile").ToArray())
			{
				element.Value = element.Value.Replace(".XML", "-3.5.XML");
			}
			foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkVersion"))
			{
				element.Value = "v3.5";
			}
			foreach (var element in database.Root.Descendants(xmlns + "TargetFrameworkProfile"))
			{
				element.Value = "Client";
			}
			foreach (var element in database.Root.Descendants(xmlns + "AssemblyName"))
			{
				element.Value += "-3.5";
			}
			using (var xmlWriter = XmlWriter.Create(destFile,
													new XmlWriterSettings
													{
														Indent = true
													}))
			{
				database.WriteTo(xmlWriter);
				xmlWriter.Flush();
			}
		}
	}
}
