//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Raven.ProjectRewriter
{
	class Program
	{
		static XNamespace xmlns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
		static void Main(string[] args)
		{
			//Generate35(@"Raven.Abstractions\Raven.Abstractions.csproj",
			//    @"Raven.Abstractions\Raven.Abstractions.g.3.5.csproj",
			//    "Raven.Json");
			//Generate35(@"Raven.Client.Lightweight\Raven.Client.Lightweight.csproj",
			//    @"Raven.Client.Lightweight\Raven.Client.Lightweight.g.3.5.csproj",
			//    "Raven.Json",
			//    "Raven.Abstractions");

			GenerateSilverlight4(@"Raven.Client.Silverlight\Raven.Client.Silverlight.csproj",
				@"Raven.Client.Silverlight\Raven.Client.Silverlight.g.4.csproj");
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
				if (element.Attribute("Include").Value == "AsyncCtpLibrary_Silverlight5")
				{
					element.Attribute("Include").Value = "AsyncCtpLibrary_Silverlight";
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					if (hintPath != null)
						hintPath.Value = hintPath.Value.Replace("Silverlight5", "Silverlight");
				}
				if (element.Attribute("Include").Value.Contains("Version=5.0.5.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35"))
				{
					element.Attribute("Include").Value = element.Attribute("Include").Value.Replace("5.0.5.0", "2.0.5.0");
					var hintPath = element.Descendants(xmlns + "HintPath").FirstOrDefault();
					if (hintPath != null)
						hintPath.Value = hintPath.Value.Replace(@"Microsoft SDKs\Silverlight\v5.0\", @"Microsoft SDKs\Silverlight\v4.0\");
				}
				if (element.Attribute("Include").Value.Contains("Version=5.0.5.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35"))
					element.Attribute("Include").Value = element.Attribute("Include").Value.Replace("5.0.5.0", "2.0.5.0");

				if (element.Attribute("Include").Value.Contains("System.Reactive, "))
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
