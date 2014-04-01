using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class RavenCannotQueryAgainstFloatProperty : RavenTestBase
	{
		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void CanQueryAgainstFloatProperties()
		{
            using (var store = NewDocumentStore())
			{
				new OperationDoc_Index().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new OperationDoc { Name = "Pippo", Quantity = 100.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Pluto", Quantity = 90.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Paperino", Quantity = 80.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Minni", Quantity = 70.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Paperoga", Quantity = 200.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Gastone", Quantity = 220.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Qui", Quantity = -10.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Quo", Quantity = -3.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Qua", Quantity = -5.0f, Timestamp = DateTime.Now });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
				    var result = session.Query<OperationDoc, OperationDoc_Index>()
				        .Customize(c => c.WaitForNonStaleResults())
				        .Where(op => op.Quantity < 0)
					                    .ToList();

					Assert.Equal(3, result.Count);
				}
			}
		}

		[Fact]
		public void CanQueryAgainstFloatProperties2()
		{
			using (var store = NewDocumentStore())
			{
				new OperationDoc_Index().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new OperationDoc { Name = "Pippo", Quantity = 100.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Pluto", Quantity = 90.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Paperino", Quantity = 80.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Minni", Quantity = 70.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Paperoga", Quantity = 200.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Gastone", Quantity = 220.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Qui", Quantity = -10.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Quo", Quantity = -3.0f, Timestamp = DateTime.Now });
					session.Store(new OperationDoc { Name = "Qua", Quantity = -5.0f, Timestamp = DateTime.Now });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<OperationDoc, OperationDoc_Index>()
										.Customize(c => c.WaitForNonStaleResults())
										.Where(op => op.Quantity >= 0)
										.ToList();

					Assert.Equal(6, result.Count);
				}
			}
		}

		public class OperationDoc
		{
			public string Id { get; set; }
			public DateTime Timestamp { get; set; }
			public string Name { get; set; }
			public float Quantity { get; set; }
		}

		public class OperationDoc_Index : AbstractIndexCreationTask<OperationDoc>
		{
			public OperationDoc_Index()
			{
				Map = ops => from op in ops
							 select new
							 {
								 op.Quantity
							 };
                Sort(x=>x.Quantity, SortOptions.Float);
			}
		}
	}
}
