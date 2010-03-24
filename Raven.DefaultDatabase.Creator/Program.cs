using System;
using System.IO;
using System.Xml;
using ICSharpCode.SharpZipLib.Zip;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Sgml;
using System.Linq;
using Formatting = Newtonsoft.Json.Formatting;

namespace Raven.DefaultDatabase.Creator
{
	class Program
	{
		static void Main()
		{
			var array = new JArray();

			var sgmlReader = new SgmlReader
			{
				Href = "http://groups.google.com/group/ravendb/web/docs-http-api-index"
			};

			var doc = new XmlDocument();
			doc.Load(sgmlReader);


			var layout = doc.SelectSingleNode("//div[@class='layout']");

			var index = new JObject(new JProperty("Html", FixLinks(layout.InnerXml)), new JProperty("Name", "Index"));

			array.Add(new JObject(
			          	new JProperty("DocId", "raven_documentation/index"),
			          	new JProperty("Document", index),
			          	new JProperty("Metadata",
			          	              new JObject(new JProperty("Raven-View-Template", "/static/raven/documentation.template")))
			          	));


			foreach (XmlNode link in layout.SelectNodes(".//a"))
			{
				ExportDocument(array, link.Attributes["href"].Value);
			}

			File.WriteAllText("default.json", array.ToString(Formatting.Indented));
		}

		private static string FixLinks(string xml)
		{
			var doc = new XmlDocument();
			doc.LoadXml(xml);

			foreach (XmlNode link in doc.SelectNodes("//a"))
			{
				if (link.Attributes["href"].Value.StartsWith("/group/ravendb/web/") == false)
					continue;
				link.Attributes["href"].Value = link.Attributes["href"].Value
					.Replace("/group/ravendb/web/", "/raven/view.html?docId=raven_documentation/")
					.Replace("-", "_");
			}
			return doc.OuterXml;
		}

		private static void ExportDocument(JArray array,string href)
		{
			Console.WriteLine("Reading {0}", href);
			var linkReader = new SgmlReader
								{
									Href = new UriBuilder("http", "groups.google.com", 80, href).Uri.ToString()
								};

			var linkDoc = new XmlDocument();
			linkDoc.Load(linkReader);

			var layout = linkDoc.SelectSingleNode("//div[@class='layout']");
			var title = layout.SelectSingleNode(".//h2").InnerText;
			var name = linkDoc.SelectSingleNode("/html/head/title").InnerText.Split('-').First().Trim();

			Console.WriteLine("Writing {0}", title);

			var index = new JObject(new JProperty("Html", FixLinks(layout.InnerXml)), new JProperty("Name", title));
			array.Add(new JObject(
						new JProperty("DocId", "raven_documentation/" + name),
						new JProperty("Document", index),
						new JProperty("Metadata",
									  new JObject(new JProperty("Raven-View-Template", "/static/raven/documentation.template")))
						));
		}
	}
}
