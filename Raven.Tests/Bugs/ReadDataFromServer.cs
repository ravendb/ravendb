//-----------------------------------------------------------------------
// <copyright file="ReadDataFromServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using System.Net;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ReadDataFromServer : RavenTest
	{
		[Fact]
		public void CanReadDataProperly()
		{
			using(GetNewServer())
			{
				using (var webClient = new WebClient())
				{
					var downloadData = webClient.DownloadData("http://localhost:8079/" +
						"indexes?pageSize=128&start=" + "0");
					var documents = GetString(downloadData);
					RavenJArray.Parse(documents);
				}
			}
		}

		private static string GetString(byte[] downloadData)
		{
			using (var ms = new MemoryStream(downloadData))
			using (var reader = new StreamReader(ms))
			{
				return reader.ReadToEnd();
			}
		}
	}
}