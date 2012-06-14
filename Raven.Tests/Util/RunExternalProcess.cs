using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Raven.Abstractions.Commands;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Util
{
	public class RunExternalProcess : IDisposable
	{
		[Fact]
		public void can_use_RavenDB_in_a_remote_process()
		{
			var documentConvention = new DocumentConvention();

			using (var driver = new RavenDBDriver("HelloShard", documentConvention))
			{
				driver.Start();

				using (var store = new DocumentStore()
				{
					Url = driver.Url,
					Conventions = documentConvention
				})
				{
					store.Initialize();

					using (var session = store.OpenSession())
					{
						session.Store(new Tuple<string, string>("hello", "world"));
						session.SaveChanges();
					}
				}

				using (var store = driver.GetDocumentStore())
				{
					should_find_expected_value_in(store);
				}
			}
		}


		[Fact]
		public void can_use_RavenDB_in_a_remote_process_for_batch_operations()
		{
			var documentConvention = new DocumentConvention();

			using (var driver = new RavenDBDriver("HelloShard", documentConvention))
			{
				driver.Start();

				using (var store = new DocumentStore()
				{
					Url = driver.Url,
					Conventions = documentConvention
				})
				{
					store.Initialize();

					using (var session = store.OpenSession())
					{
						store.DatabaseCommands.Batch(new[] { GetPutCommand() });
						session.SaveChanges();
					}
				}

				using (var store = driver.GetDocumentStore())
				{
					should_find_expected_value_in(store);
				}
			}
		}


		[Fact]
		public void can_use_RavenDB_in_a_remote_process_to_post_batch_operations()
		{
			var documentConvention = new DocumentConvention();

			using (var driver = new RavenDBDriver("HelloShard", documentConvention))
			{
				driver.Start();

				var httpWebRequest = (HttpWebRequest)WebRequest.Create(new Uri(new Uri(driver.Url), "bulk_docs"));
				httpWebRequest.Method = "POST";
				using (var requestStream = new StreamWriter(httpWebRequest.GetRequestStream(), Encoding.UTF8))
				{
					requestStream.Write("[");

					requestStream.Write(GetPutCommand().ToJson().ToString());

					requestStream.Write("]");
				}

				HttpWebResponse webResponse;

				try
				{
					webResponse = (HttpWebResponse)httpWebRequest.GetResponse();
					webResponse.Close();
				}
				catch (WebException e)
				{
					driver.Should_finish_without_error();
					driver.TraceExistingOutput();

					Console.WriteLine(new StreamReader(e.Response.GetResponseStream()).ReadToEnd());
					throw;
				}

				using (var store = driver.GetDocumentStore()) 
				{
					should_find_expected_value_in(store);
				}
			}
		}

		PutCommandData GetPutCommand()
		{
			return new PutCommandData()
			{
				Document = RavenJObject.FromObject(new Tuple<string, string>("hello", "world")),
				Metadata = new RavenJObject
				{
					{"Raven-Entity-Name", new RavenJValue("TuplesOfStringsOfStrings")},
					{"Raven-Clr-Type", new RavenJValue("System.Tuple`2[[System.String, mscorlib][System.String, mscorlib]], mscorlib")}
				}
			};
		}

		void should_find_expected_value_in(IDocumentStore store)
		{
			using (var session = store.OpenSession())
			{
				var result = session.Query<Tuple<string, string>>().Customize(q => q.WaitForNonStaleResultsAsOfNow()).Single();

				Assert.Equal("hello", result.Item1);
				Assert.Equal("world", result.Item2);
			}
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public void Dispose()
		{
			IOExtensions.DeleteDirectory("HelloShard");
		}
	}
}