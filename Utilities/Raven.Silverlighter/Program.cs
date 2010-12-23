using System.Xml;
using System.Xml.Linq;

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
				new XElement(ns+"TargetFrameworkVersion", "v4.0"),
				new XElement(ns + "SilverlightVersion", "$(TargetFrameworkVersion)")
				);

			var xmlWriter = XmlWriter.Create(args[1]);
			srcPrj.WriteTo(xmlWriter);
			xmlWriter.Flush();
		}
	}
}
