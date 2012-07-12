using System;
using Raven.Client.Bundles.Versioning;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Bundles.Tests.Versioning.Bugs
{
	public class VersioningWithGuidIds : VersioningTest
	{
		[Fact]
		public void Loading_versioned_entity_with_guid_id_will_fail()
		{
			documentStore.Conventions.FindIdValuePartForValueTypeConversion = (entity, id) =>
			{
				var parts = id.Split('/');
				var guid = parts[1];
				if(parts.Length==4)
				{
					((EntityWithGuidId) entity).Revision = int.Parse(parts[3]);
				}
				return guid;
			};

			Guid entityId = Guid.NewGuid();
			using (var session = documentStore.OpenSession())
			{
				var entity = new EntityWithGuidId()
				{
					Id = entityId,
					Data = "initial version"
				};
				session.Store(entity);
				session.SaveChanges();
			}
			using (var session = documentStore.OpenSession())
			{
				var entity = session.Load<EntityWithGuidId>(entityId);
				entity.Data = "first revision";
				session.SaveChanges();
			}
			using (var session = documentStore.OpenSession())
			{
				var entity = session.Load<EntityWithGuidId>(entityId);
				string ravenId = session.Advanced.GetDocumentId(entity);

				Assert.DoesNotThrow(
						() =>
						{
							var revisions = session.Advanced.GetRevisionsFor<EntityWithGuidId>(ravenId, 0, 10);
						}
				);
			}
		}

		public class EntityWithGuidId
		{
			public Guid Id { get; set; }
			public string Data { get; set; }

			[JsonIgnore]
			public int Revision { get; set; }
		}
	}
}