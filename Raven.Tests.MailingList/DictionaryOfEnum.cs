// -----------------------------------------------------------------------
//  <copyright file="DictionaryOfEnum.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class DictionaryOfEnum : RavenTest
	{
		public enum MyEnum
		{
			Value1,
			Value2
		}

		public class Test
		{
			public string Id;
			public Dictionary<MyEnum, string> Name { get; set; }
		}

		public class Result
		{
			public string Id;
			public string Name;
		}

		public class MyTransformer : AbstractTransformerCreationTask<Test>
		{
			public MyTransformer()
			{
				TransformResults = results =>
					from result in results
					select new
					{
						Name = result.Name.FirstOrDefault(a => a.Key == MyEnum.Value1).Value
					};
			}
		}

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				new MyTransformer().Execute(store);
				using (var s = store.OpenSession())
				{
					s.Store(new Test
					{
						Name = new Dictionary<MyEnum, string>
						{
							{MyEnum.Value1, "t"},
							{MyEnum.Value2, "b"}
						}
					});
					s.SaveChanges();

					var myTransformer = s.Load<MyTransformer, Result>("tests/1");
					Assert.Equal("t", myTransformer.Name);
				}


			}
		}
 
	}
}