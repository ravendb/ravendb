//-----------------------------------------------------------------------
// <copyright file="ReadDataFromServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Server;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ReadDataFromServer : AbstractDocumentStorageTest
	{
		[Fact]
		public void CanReadDataProperly()
		{
			using(new RavenDbServer(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true}))
			{
				using (var webClient = new WebClient())
				{
					var downloadData = webClient.DownloadData("http://localhost:8080/" +
						"indexes?pageSize=128&start=" + "0");
					var documents = Smuggler.Smuggler.GetString(downloadData);
					JArray.Parse(documents);
				}
			}

		}
	}

}
