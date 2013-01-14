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
			var slPrj = XDocument.Load(args[0]);


			foreach (var compile in slPrj.Descendants(ns+"Compile").ToArray())
			{
				if(compile.Element(ns+"Link") != null)
					compile.Remove();
			}

			foreach (var prjFile in args.Skip(1))
			{
				var srcPrj = XDocument.Load(prjFile);

				foreach (var file in srcPrj.Descendants(ns + "Compile"))
				{
					string filePath = file.Attribute("Include").Value;


					if (slPrj.Descendants(ns + "Compile")
						.Any(x => x.Attribute("Include") != null && x.Attribute("Include").Value == filePath))
						continue;

					string newFilePath = Path.Combine(@"..", Path.GetFileName(Path.GetDirectoryName(prjFile)), filePath);
					slPrj.Descendants(ns + "ItemGroup").Last().Add(
						new XElement(ns + "Compile",
							new XAttribute("Include", newFilePath),
							new XElement(ns + "Link", filePath)
							)
						);
				}
			}

			XmlWriter xmlWriter = XmlWriter.Create(args[0], new XmlWriterSettings
			{
				Indent = true
			});
			slPrj.WriteTo(xmlWriter);
			xmlWriter.Flush();
		}
	}
}
