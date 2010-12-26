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
			var slPrj = XDocument.Load(args[1]);

			foreach (var file in srcPrj.Descendants(ns+"Compile"))
			{
				string filePath = file.Attribute("Include").Value;

				if(slPrj.Descendants(ns + "Compile")
					.Any(x => x.Element(ns + "Link") != null && x.Element(ns + "Link").Value == filePath))
					continue;

				string newFilePath = Path.Combine(@"..", Path.GetFileName(Path.GetDirectoryName(args[0])), filePath);
				slPrj.Descendants(ns+"ItemGroup").Last().Add(
					new XElement(ns+"Compile",
						new XAttribute("Include",newFilePath),
						new XElement(ns+"Link", filePath)
						)
					);
			}

			XmlWriter xmlWriter = XmlWriter.Create(args[1], new XmlWriterSettings
			{
				Indent = true
			});
			slPrj.WriteTo(xmlWriter);
			xmlWriter.Flush();
		}
	}
}
