using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Database.Extensions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Document
{
	public class DocumentStoreEmbeddedGranularTests : RavenTest, IDisposable
	{
		private string path;

		[Fact]
		public void Should_retrieve_all_entities_using_connection_string()
		{
			using (var documentStore = new EmbeddableDocumentStore
			{
				ConnectionStringName = "Local",
				Configuration =
				{
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
				}
			})
			{
				path = documentStore.DataDirectory;

				documentStore.Initialize();

				var session1 = documentStore.OpenSession();
				session1.Store(new Company { Name = "Company 1" });
				session1.Store(new Company { Name = "Company 2" });

				session1.SaveChanges();
				var session2 = documentStore.OpenSession();
                var companyFound = session2.Advanced.DocumentQuery<Company>()
					.WaitForNonStaleResults()
					.ToArray();

				Assert.Equal(2, companyFound.Length);
			}
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}
	}
}