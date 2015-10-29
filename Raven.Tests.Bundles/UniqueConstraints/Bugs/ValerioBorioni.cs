// //-----------------------------------------------------------------------
// // <copyright file="Troy.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;

using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
	public class ValerioBorioni : UniqueConstraintsTest
	{
		public class MyEntity
		{
            public string Id { get; set; }
            [UniqueConstraint(CaseInsensitive = true)]
			public string ExternalReference { get; set; }
		}

		[Fact]
		public void LoadByUniqueConstraintDocumentStoredOnCurrentSession()
		{
            string reference = "value";
            var entity = new MyEntity { ExternalReference = reference };

            using (var session = DocumentStore.OpenSession())
			{
                session.LoadByUniqueConstraint<MyEntity>(r => r.ExternalReference, reference);
                session.Store(entity);
				session.SaveChanges();
                var loadedEntity = session.LoadByUniqueConstraint<MyEntity>(r => r.ExternalReference, reference);
                Assert.Equal(entity.Id, loadedEntity.Id);
			}
			using (var session = DocumentStore.OpenSession())
			{
                var loadedEntity = session.LoadByUniqueConstraint<MyEntity>(r => r.ExternalReference, reference);
                Assert.Equal(entity.Id, loadedEntity.Id);
            }
		}

	}
}