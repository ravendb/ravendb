using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using Newtonsoft.Json.Linq;
using Sgml;
using Formatting = Newtonsoft.Json.Formatting;

namespace Raven.DefaultDatabase.Creator
{
	class Program
	{
		static void Main(string[] args)
		{
			var array = new JArray();
			var crawled = new HashSet<string>();
			var sgmlReader = new SgmlReader
			{
				Href = "http://groups.google.com/group/ravendb/web/docs-http-api-index"
			};
			crawled.Add(sgmlReader.Href);
			var doc = new XmlDocument();
			doc.Load(sgmlReader);


			var layout = doc.SelectSingleNode("//div[@class='layout']");

			var index = new JObject(new JProperty("Html", FixLinks(layout.InnerXml)), new JProperty("Name", "Index"));

			array.Add(new JObject(
			          	new JProperty("DocId", "raven_documentation/index"),
			          	new JProperty("Document", index),
						new JProperty("Type", "raven documentation"),
			          	new JProperty("Metadata",
			          	              new JObject(new JProperty("Raven-View-Template", "/raven/JSONTemplates/documentation.html")))
			          	));


			AddDocumentsFromLinks(array, crawled, layout.SelectNodes(".//a"));

			File.WriteAllText(args[0], array.ToString(Formatting.Indented));
		}

		private static void AddDocumentsFromLinks(JArray array, HashSet<string> crawled, XmlNodeList list)
		{
			foreach (XmlNode link in list)
			{
				var href = link.Attributes["href"].Value;
				if (href.IndexOf("group/ravendb/web/", StringComparison.InvariantCultureIgnoreCase) != -1)
					ExportDocument(array, crawled, href);
			}
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

		private static void ExportDocument(JArray array, HashSet<string> crawled, string href)
		{
			if (crawled.Add(href) == false)
				return;

			Console.WriteLine("Reading {0}", href);
			var uri = new UriBuilder("http", "groups.google.com", 80, href).Uri;
			var linkReader = new SgmlReader
								{
									Href = uri.ToString()
								};
			
			var linkDoc = new XmlDocument();
			linkDoc.Load(linkReader);


			var layout = linkDoc.SelectSingleNode("//div[@class='layout']");
			var title = layout.SelectSingleNode(".//h2").InnerText;
			var name = Path.GetFileName(uri.LocalPath).Replace("-", "_");

			AddDocumentsFromLinks(array, crawled, layout.SelectNodes(".//a"));


			Console.WriteLine("Writing {0}", title);

			var index = new JObject(new JProperty("Html", FixLinks(layout.InnerXml)), new JProperty("Name", title));
			array.Add(new JObject(
						new JProperty("DocId", "raven_documentation/" + name),
						new JProperty("Document", index),
						new JProperty("Type", "raven documentation"),
						new JProperty("Metadata",
									  new JObject(new JProperty("Raven-View-Template", "/raven/JSONTemplates/documentation.html")))
						));
		}
	}
}
