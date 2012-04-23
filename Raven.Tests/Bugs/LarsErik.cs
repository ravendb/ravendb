using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class LarsErik : RavenTest
	{
		[Fact]
		public void ShouldBeAbleToDeserializeSubEntities()
		{
			using(GetNewServer())
			{
				var entity = new ComplexEntity("test");
				entity.AddSub(new SubEntity());
				entity.AddSub(new SubEntity());
				entity.SetCurrentSub(entity.Subs[0]);
				var id = entity.Id;

				using (DocumentStore store = CreateAndInitializeStore())
				{
					using (IDocumentSession session = store.OpenSession())
					{
						session.Store(entity);
						session.SaveChanges();
					}

				}
				using (DocumentStore store = CreateAndInitializeStore())
				{
					using (IDocumentSession session = store.OpenSession())
					{
						try
						{
							ComplexEntity readEntity = session.Load<ComplexEntity>(id);
							// fails... see CreateAndInitializeStore() for passing
							Assert.Equal(2, readEntity.SubCount);
						}
						finally
						{
							ComplexEntity readEntity = session.Query<ComplexEntity>().First(c => c.Id == id);
							session.Delete(readEntity);
							session.SaveChanges();
						}
					}
				}
			}
		}

		private static DocumentStore CreateAndInitializeStore()
		{
			const string server = "http://localhost:8079";

			DocumentStore store = new DocumentStore { Url = server };

			CustomRavenContractResolver contractResolver = new CustomRavenContractResolver(true)
			{
				DefaultMembersSearchFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
			};

			contractResolver.IgnoreMember<ComplexEntity>(typeof(ComplexEntity).GetProperty("SubCount"));
			// omit to make test pass
			store.Conventions.JsonContractResolver = contractResolver;

		

			store.Initialize();

			return store;
		}

		public class ComplexEntity
		{
			private List<SubEntity> subs = new List<SubEntity>();
			public Guid Id { get; private set; }

			public int SubCount
			{
				get { return subs.Count; }
			}

			public string Name { get; set; }

			public SubEntity CurrentSub { get; private set; }

			public IList<SubEntity> Subs
			{
				get { return subs.AsReadOnly(); }
				private set { subs = value.ToList(); }
			}

			public ComplexEntity(string name)
			{
				Id = Guid.NewGuid();
				Name = name;
			}

			public void AddSub(SubEntity sub)
			{
				subs.Add(sub);
			}

			public void SetCurrentSub(SubEntity sub)
			{
				CurrentSub = sub;
			}
		}

		public class SubEntity
		{
			public Guid Id { get; private set; }

			public SubEntity()
			{
				Id = Guid.NewGuid();
			}
		}

		public class CustomRavenContractResolver : DefaultRavenContractResolver
		{
			private Dictionary<Type, List<MemberInfo>> ignoreMembers = new Dictionary<Type, List<MemberInfo>>();

			public CustomRavenContractResolver(bool shareCache)
				: base(shareCache)
			{
			}

			public void IgnoreMember<T>(MemberInfo member)
			{
				Type type = typeof(T);
				if (!ignoreMembers.ContainsKey(type))
					ignoreMembers.Add(type, new List<MemberInfo>());
				if (!ignoreMembers[type].Contains(member))
					ignoreMembers[type].Add(member);
			}

			protected override List<MemberInfo> GetSerializableMembers(Type objectType)
			{
				List<MemberInfo> members = base.GetSerializableMembers(objectType);
				if (ignoreMembers.ContainsKey(objectType))
					foreach (MemberInfo member in ignoreMembers[objectType])
						members.Remove(member);
				return members;
			}
		}

	}
}