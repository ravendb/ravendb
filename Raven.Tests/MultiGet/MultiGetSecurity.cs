using System.IO;
using System.Net;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MultiGet
{
	public class MultiGetSecurity : RemoteClientTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.AnonymousUserAccessMode =AnonymousUserAccessMode.Get;
		}

		[Fact]
		public void CanUseMultiGetToBatchGetDocumentRequests_WhenAnonymousAccessEqualsToGet()
		{
			using (GetNewServer())
			using (var docStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = docStore.OpenSession())
				{
					session.Store(new User { Name = "Ayende" });
					session.Store(new User { Name = "Oren" });
					session.SaveChanges();
				}

				var request = (HttpWebRequest)WebRequest.Create("http://localhost:8079/multi_get");
				request.Method = "POST";
				using (var stream = request.GetRequestStream())
				{
					var streamWriter = new StreamWriter(stream);
					JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
					{
						new GetRequest
						{
							Url = "/docs/users/1"
						},
						new GetRequest
						{
							Url = "/docs/users/2"
						},
					});
					streamWriter.Flush();
					stream.Flush();
				}

				using (var resp = request.GetResponse())
				using (var stream = resp.GetResponseStream())
				{
					var result = new StreamReader(stream).ReadToEnd();
					Assert.Contains("Ayende", result);
					Assert.Contains("Oren", result);
				}
			}
		}
	}
}