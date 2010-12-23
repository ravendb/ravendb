using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Linq;

namespace Raven.Silverlighter
{
	class Program
	{
		static void Main(string[] args)
		{
			var ns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
			var srcPrj = XDocument.Load(args[0]);
			var propGroup = srcPrj.Element(ns + "Project").Element(ns + "PropertyGroup");
			propGroup.Element(ns + "AssemblyName").Value += ".Silverlight";
			propGroup.Add(
				new XElement(ns + "ProjectTypeGuids", "{A1591282-1198-4647-A2B1-27E5FF5F6F3B};{fae04ec0-301f-11d3-bf4b-00c04f79efbc}"),
				new XElement(ns + "TargetFrameworkVersion", "v4.0"),
				new XElement(ns + "SilverlightVersion", "$(TargetFrameworkVersion)")
				);

			foreach (var s in srcPrj.Descendants(ns + "Reference").ToArray())
			{
				var xElement = s.Element(ns + "HintPath");
				if (xElement == null)
					continue;
				if (File.Exists(Path.GetFileNameWithoutExtension(xElement.Value) + ".Silverlight.dll") == false)
					continue;
				var filePath = Path.GetFileName(Path.GetFileNameWithoutExtension(xElement.Value)) + ".Silverlight.dll";
				xElement.Parent.Add(
					new XElement(ns + "Reference",
						new XAttribute("Include", Path.GetFileName(Path.GetFileNameWithoutExtension(xElement.Value)) + ".Silverlight"),
						new XElement("HintPath",filePath)
						)
					);
				s.Remove();
			}

			foreach (var s in srcPrj.Descendants(ns + "ProjectReference").ToArray())
			{
				s.Attribute("Include").Value = s.Attribute("Include").Value.Replace(".csproj", ".Silverlight.g.csproj");
				s.Element(ns + "Name").Value += ".Silverlight";
			}

			var xmlWriter = XmlWriter.Create(args[1], new XmlWriterSettings
			{
				Indent = true,
			});
			srcPrj.WriteTo(xmlWriter);
			xmlWriter.Flush();
		}
	}
}
