using System;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace Raven.ProjectRewriter
{
	class Program
	{
		static void Main()
		{
			var xmlns = XNamespace.Get("http://schemas.microsoft.com/developer/msbuild/2003");
			var database = XDocument.Load(@"Raven.Database\Raven.Database.csproj");
			foreach (var element in database.Root.Descendants(xmlns+"DefineConstants").ToArray())
			{
				if (element.Value.EndsWith(";") == false)
					element.Value += ";";
				element.Value += "COMMERCIAL";
			}
			using (var xmlWriter = XmlWriter.Create(@"Raven.Database\Raven.Database.g.csproj",
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
