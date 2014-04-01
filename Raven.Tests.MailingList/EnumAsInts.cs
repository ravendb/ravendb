using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class EnumAsInts : RavenTest
	{
		public enum Flags
		{
			One = 1,
			Two = 2,
			Four = 4
		}
		public class Item
		{
			public Flags Flags { get; set; }
		}
		public class Index : AbstractIndexCreationTask<Item>
		{
			public Index()
			{
				Map = items => from item in items
				               where (item.Flags & Flags.Four) == Flags.Four
				               select new {item.Flags};
			}
		}

		[Fact]
		public void CanWork()
		{
			using(var store = NewDocumentStore())
			{
				store.Conventions.SaveEnumsAsIntegers = true;
				new Index().Execute(store);
			}
		}
	}
}