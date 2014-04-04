using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class DataContracts : RavenTest
	{
		[Fact]
		public void RespectsNameOnDataMemberAttribute()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new DataContractClass { Name = "tname" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<DataContractClass>()
						.Where(tc => tc.Name.Equals("tname"))
						.FirstOrDefault();

					Assert.NotNull(result);
					Assert.Equal("tname", result.Name);
				}
			}
		}

		[Fact]
		public void RespectsNameOnDataMemberAttributeWithNestedCollection()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new WrapperClass
					{
						Children = new List<DataContractClass>
						{
							new DataContractClass{ Name = "tname"}
						}
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<WrapperClass>()
						.Where(wc => wc.Children.Any(c => c.Name.Equals("tname")))
						.FirstOrDefault();

					Assert.NotNull(result);
					Assert.Equal("tname", result.Children.FirstOrDefault().Name);
				}
			}
		}

		[DataContract]
		public class WrapperClass
		{
			[DataMember]
			public string Id { get; set; }

			[DataMember(Name="offspring")]
			public ICollection<DataContractClass> Children { get; set; }
		}

		[DataContract]
		public class DataContractClass
		{
			[DataMember]
			public string Id { get; set; }

			[DataMember(Name="n")]
			public string Name { get; set; }
		}
	}
}
