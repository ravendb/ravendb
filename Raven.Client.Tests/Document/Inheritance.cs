using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Document
{
	public class Inheritance : BaseTest, IDisposable
	{
		private string path;

		#region IDisposable Members

		public void Dispose()
		{
			Directory.Delete(path, true);
		}

		#endregion


		private DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			var documentStore = new DocumentStore
			{
				Configuration =
					{
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction =true,
						DataDirectory = path
					},
				Conventions =
					{
						FindTypeTagName = type => typeof(IServer).IsAssignableFrom(type) ? "Servers" : null
					}
			};
			documentStore.Initialize();
			return documentStore;
		}

		[Fact]
		public void CanStorePolymorphicTypesAsDocuments()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new WindowsServer
					{
						ProductKey = Guid.NewGuid().ToString()
					});
					session.Store(new LinuxServer
					{
						KernelVersion = "2.6.7"
					});
					session.SaveChanges();

					IServer[] servers = session.LuceneQuery<IServer>()
						.WaitForNonStaleResults()
						.ToArray();
					Assert.Equal(2, servers.Length);
				}
			}
		}

		public class WindowsServer  : IServer
		{
			public string Id { get; set; }
			public string ProductKey { get; set; }

			public void Start()
			{
				
			}
		}

		public class LinuxServer : IServer
		{
			public string Id { get; set; }
			public string KernelVersion { get; set; }
			
			public void Start()
			{
				
			}
		}

		public interface IServer
		{
			void Start();
		}
	}

	
}