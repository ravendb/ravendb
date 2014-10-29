// -----------------------------------------------------------------------
//  <copyright file="WithNullableDateTime.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Indexes {
	public class WithNullableDateTime : RavenTest 
	{
		[Fact]
		public void CanCreate() 
		{
			using (var documentStore = this.NewDocumentStore()) {
				new FooIndex().Execute(documentStore);
				
				using (var session = documentStore.OpenSession()) {
					session.Store(new Foo {NullableDateTime = new DateTime(1989, 3, 15, 7, 28, 1, 1)});
					session.SaveChanges();
					
					Assert.NotNull(
						session.Query<Foo, FooIndex>()
							   .Customize(c => c.WaitForNonStaleResults())
							   .FirstOrDefault());
				}
			}
		}

		public class FooIndex : AbstractIndexCreationTask<Foo>  
		{
			public FooIndex() {
				this.Map =
					docs => from doc in docs
							where doc.NullableDateTime != null
							select new
							{
								doc.NullableDateTime.GetValueOrDefault().Date,
							};
			}
		}

		public class Foo 
		{
			public DateTime? NullableDateTime { get; set; }
		}
	}
}