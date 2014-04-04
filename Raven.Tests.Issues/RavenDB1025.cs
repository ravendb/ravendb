// -----------------------------------------------------------------------
//  <copyright file="RavenDB1025.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB1025 : RavenTest
	{
		public class MyClass
		{
			public int Index { get; set; }
			[JsonProperty(PropertyName = "S")]
			public IList<double> Statistics { get; set; }
		}

		[Fact]
		public void CanSaveAndProject()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new MyClass { Index = 1, Statistics = new List<double> { 1, 3, 4 } });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<MyClass>()
					  .Customize(x=>x.WaitForNonStaleResults())
					  .Select(x => new MyClass
					  {
						  Index = x.Index,
						  Statistics= x.Statistics,
					  }).Single();

					  Assert.NotNull(results.Statistics);
				}

			}
		}
	}
}