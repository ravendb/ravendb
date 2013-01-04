using System;
using System.IO;
using System.Net;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;
using Raven.Abstractions;

namespace Raven.Tests.MultiGet
{
	public class MultiGetBasic : RemoteClientTest
	{
		[Fact]
		public void CanUseMultiGetToBatchGetDocumentRequests()
		{
			using(GetNewServer())
			using(var docStore = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using(var session = docStore.OpenSession())
				{
					session.Store(new User{Name = "Ayende"});
					session.Store(new User{Name = "Oren"});
					session.SaveChanges();
				}

				var request = (HttpWebRequest)WebRequest.Create("http://localhost:8079/multi_get");
				request.Method = "POST";
				using(var stream = request.GetRequestStream())
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

				using(var resp = request.GetResponse())
				using (var stream = resp.GetResponseStream())
				{
					var result = new StreamReader(stream).ReadToEnd();
					Assert.Contains("Ayende", result);
					Assert.Contains("Oren", result);
				}
			}
		}

		[Fact]
		public void CanUseMultiQuery()
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

				using (var session = docStore.OpenSession())
				{
					session.Query<User>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(u=>u.Name == "Ayende")
						.ToArray();
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
							Url = "/indexes/dynamic/Users",
							Query = "query=Name:Ayende"
						},
						new GetRequest
						{
							Url = "/indexes/dynamic/Users",
							Query = "query=Name:Oren"
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

		[Fact]
		public void CanHandleCaching()
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
				
				GetResponse[] results;
				using (var resp = request.GetResponse())
				using (var stream = resp.GetResponseStream())
				{
					var result = new StreamReader(stream).ReadToEnd();
					results = JsonConvert.DeserializeObject<GetResponse[]>(result, Default.Converters);
					Assert.True(results[0].Headers.ContainsKey("ETag"));
					Assert.True(results[1].Headers.ContainsKey("ETag"));
				}

				request = (HttpWebRequest)WebRequest.Create("http://localhost:8079/multi_get");
				request.Method = "POST";
				using (var stream = request.GetRequestStream())
				{
					var streamWriter = new StreamWriter(stream);
					JsonExtensions.CreateDefaultJsonSerializer().Serialize(streamWriter, new[]
					{
						new GetRequest
						{
							Url = "/docs/users/1",
							Headers =
					                                             	{
					                                             		{"If-None-Match", results[0].Headers["ETag"]}
					                                             	}
						},
						new GetRequest
						{
							Url = "/docs/users/2",
							Headers =
					                                             	{
					                                             		{"If-None-Match", results[1].Headers["ETag"]}
					                                             	}
						},
					});
					streamWriter.Flush();
					stream.Flush();
				}

				using (var resp = request.GetResponse())
				using (var stream = resp.GetResponseStream())
				{
					var result = new StreamReader(stream).ReadToEnd();
					results = JsonConvert.DeserializeObject<GetResponse[]>(result, Default.Converters);
					Assert.Equal(304, results[0].Status);
					Assert.Equal(304, results[1].Status);
				}
			}
		}
	}
}