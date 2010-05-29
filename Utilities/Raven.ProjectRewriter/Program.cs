using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;

namespace Raven.ProjectRewriter
{
	class Program
	{
		static void Main()
		{
			Environment.CurrentDirectory = @"C:\Work\ravendb\";
			var database = XDocument.Load(@"Raven.Database\Raven.Database.csproj");
			foreach (var element in database.Root.Descendants().ToArray())
			{
				var include = element.Attribute("Include");
				if(include != null &&  include.Value.Contains("Commercial"))
				{
					element.Remove();
				}
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
