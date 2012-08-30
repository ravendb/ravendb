//-----------------------------------------------------------------------
// <copyright file="UsingLongAsId.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class UsingLongAsId : RavenTest
	{
		[Fact]
		public void Can_use_long_as_id()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var entity = new Sun{Name = "Terra"};
					s.Store(entity);

					s.SaveChanges();

					Assert.Equal(1, entity.Id);
					Assert.Equal("suns/1", s.Advanced.GetDocumentId(entity));
				}
			}
			
		}

		public class Sun
		{
			public long Id { get; set; }
			public string Name { get; set; }
		}
	}
}
