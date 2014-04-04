using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class EnumInIndexDef : RavenTest
	{
		[Fact]
		public void QueryById()
		{
			using (var store = NewDocumentStore())
			{
				new SomeDocumentIndex().Execute(store);
			}
		}

		public class SomeDocument
		{
			public string Id { get; set; }
			public string Text { get; set; }
		}

		public enum SomeEnum
		{
			Value1 = 1,
			Value2 = 2
		}

		public class SomeDocumentIndex : AbstractIndexCreationTask<SomeDocument, SomeDocumentIndex.IndexResult>
		{
			public class IndexResult
			{
				public string Id { get; set; }
				public SomeEnum SomeEnum { get; set; }
			}

			public SomeDocumentIndex()
			{
				Map = docs => from doc in docs
							  select new { Id = doc.Id, SomeEnum = SomeEnum.Value1 };

				Store(x => x.SomeEnum, FieldStorage.Yes);
			}
		}
	}
}