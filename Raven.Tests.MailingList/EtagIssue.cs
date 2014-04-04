using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class EtagIssue : RavenTest
	{
		#region Domain

		public enum CredentialsStatus
		{
			Inactive,
			Active
		}

		public enum PublishStatus
		{
			Unpublished,
			Published
		}

		public enum RelationStatus
		{
			Active,
			Inactive
		}

		public class Credentials
		{
			public CredentialsStatus Status { get; set; }
		}

		public sealed class HasManager : Relation
		{
			internal HasManager(UserProfile user)
				: base(user)
			{
			}

			private HasManager()
			{
			}
		}

		public class InnovationProfile : Profile
		{
			public OrganizationProfile Owner { get; set; }
			public PublishStatus Status { get; set; }
		}

		public sealed class ManagerOf : Relation
		{
			internal ManagerOf(OrganizationProfile organization)
				: base(organization)
			{
			}

			private ManagerOf()
			{
			}
		}

		public class OrganizationProfile : Profile
		{
		}

		public class Profile
		{
			public Profile()
			{
				Relations = new List<Relation>();
			}

			public string Id { get; set; }

			public string Name { get; set; }
			public string Slug { get; set; }

			public List<Relation> Relations { get; set; }
		}

		public abstract class Relation
		{
			protected Relation(
				Profile to,
				string subType = null)
				: this()
			{
				To = to;
				SubType = subType;
			}

			protected internal Relation()
			{
				Type = GetType().Name;
			}

			public string Type { get; protected internal set; }
			public string SubType { get; protected internal set; }

			public Profile To { get; private set; }

			public RelationStatus Status { get; set; }
			protected internal string ActivationToken { get; set; }
		}

		public class UserManagedItem
		{
			public string Id { get; set; }
			public string Name { get; set; }

			public string OwnerId { get; set; }
			public bool IsManagedDirectly { get; set; }

			public PublishStatus PublishStatus { get; set; }
			public RelationStatus RelationStatus { get; set; }
		}

		public class UserProfile : Profile
		{
		}

		#endregion

		#region Index

		public class UserProfileIndex : AbstractMultiMapIndexCreationTask<UserProfileIndex.Result>
		{
			public UserProfileIndex()
			{
				AddMap<UserProfile>(docs =>
					from doc in docs
					select new ReduceResult
					{
						Id = doc.Id,
						Slug = doc.Slug,
						Status = LoadDocument<Credentials>(doc.Id + "/credentials").Status,
						DateUpdated = MetadataFor(doc).Value<DateTime>("Last-Modified"),
						ManagedItems =
							from r in doc.Relations
							where r.Type == "CaseManagerOf"
							let profile = LoadDocument<InnovationProfile>(r.To.Id)
							select new UserManagedItem
							{
								Id = r.To.Id,
								Name = r.To.Name,
								OwnerId = profile.Owner.Id,
								IsManagedDirectly = true,
								PublishStatus = profile.Status,
								RelationStatus = r.Status,
							},
						ManagedItems_RelationStatus = null
					});

				AddMap<InnovationProfile>(docs =>
					from doc in docs
					let caseManagerId = doc.Relations.SingleOrDefault(r => r.Type == "HasCaseManager").To.Id
					let org = LoadDocument<OrganizationProfile>(doc.Owner.Id)
					// count org managers as innovation managers if they are not case managers already
					from r in org.Relations.Where(r => r.Type == "HasManager" && r.SubType == null && r.To.Id != caseManagerId)
					select new ReduceResult
					{
						Id = r.To.Id,
						Slug = null,
						Status = CredentialsStatus.Inactive,
						DateUpdated = DateTime.MinValue,
						ManagedItems = new[]
						{
							new UserManagedItem
							{
								Id = doc.Id,
								Name = doc.Name,
								OwnerId = doc.Owner.Id,
								IsManagedDirectly = false,
								PublishStatus = doc.Status,
								RelationStatus = r.Status
							}
						},
						ManagedItems_RelationStatus = null
					});

				Reduce = results =>
					from r in results
					group r by r.Id
					into g
					let u = g.First(it => it.DateUpdated != DateTime.MinValue)
					select new ReduceResult
					{
						Id = g.Key,
						Slug = u.Slug,
						Status = u.Status,
						DateUpdated = u.DateUpdated,
						ManagedItems = g.SelectMany(it => it.ManagedItems).Distinct(),
						ManagedItems_RelationStatus = g.SelectMany(it => it.ManagedItems).Select(it => it.RelationStatus)
					};

				Index(r => r.Id, FieldIndexing.NotAnalyzed);
				Store(r => r.Id, FieldStorage.Yes);
				Store(r => r.Slug, FieldStorage.Yes);
				Store(r => r.DateUpdated, FieldStorage.Yes);
				Store(r => r.ManagedItems, FieldStorage.Yes);
			}

			internal class ReduceResult : Result
			{
				public IEnumerable<RelationStatus> ManagedItems_RelationStatus { get; set; }
			}

			public class Result
			{
				public string Id { get; set; }
				public string Slug { get; set; }
				public CredentialsStatus Status { get; set; }
				public DateTime DateUpdated { get; set; }

				public IEnumerable<UserManagedItem> ManagedItems { get; set; }
			}
		}

		#endregion

		[Fact]
		public void ScriptedPatchShouldNotResultInConcurrencyExceptionForNewlyInsertedDocument()
		{
			using (var store = NewDocumentStore())
			{
				new UserProfileIndex().Execute(store);
				store.RegisterListener(new Holt.NonStaleQueryListener());

				OrganizationProfile[] organizations =
				{
					new OrganizationProfile {Id = "organizations/1", Name = "University of California"},
					new OrganizationProfile {Id = "organizations/2", Name = "University of Stanford"}
				};

				var orgAdmin1 = new UserProfile {Id = "users/1"};
				var orgAdmin2 = new UserProfile {Id = "users/2"};

				var orgAdmins = new[]
				{
					new {User = orgAdmin1, Org = organizations[0]},
					new {User = orgAdmin2, Org = organizations[1]}
				};

				Store(store, organizations);
				Store(store, new[] {orgAdmin1, orgAdmin2});

				WaitForIndexing(store);

				IEnumerable<ScriptedPatchCommandData> patches = orgAdmins.SelectMany(orgAdmin =>
					Establish(orgAdmin.User, new ManagerOf(orgAdmin.Org),
						orgAdmin.Org, new HasManager(orgAdmin.User)));

				BatchResult[] results = ((IDocumentStore) store).DatabaseCommands.Batch(patches.ToArray());
				if (results.Any(r => r.PatchResult.Value != PatchResult.Patched))
					throw new InvalidOperationException("Some patches failed");
			}
		}

		public IEnumerable<ScriptedPatchCommandData> Establish(UserProfile user, ManagerOf managerOf,
			OrganizationProfile organization, HasManager hasManager)
		{
			const string patchScript = "var _this = this;" +
			                           "function addRelation(clrType, relation, thisArg) {" +
			                           "   (thisArg || _this).Relations.push(_.extend({ '$type': clrType }, relation));" +
			                           "}" + "addRelation(relationClrType, relation);";

			yield return new ScriptedPatchCommandData
			{
				Key = user.Id,
				Patch = new ScriptedPatchRequest
				{
					Script = patchScript,
					Values = new Dictionary<string, object>
					{
						{"relation", managerOf},
						{"relationClrType", ClrType(managerOf.GetType())}
					}
				}
			};

			yield return new ScriptedPatchCommandData
			{
				Key = organization.Id,
				Patch = new ScriptedPatchRequest
				{
					Script = patchScript,
					Values = new Dictionary<string, object>
					{
						{"relation", hasManager},
						{"relationClrType", ClrType(hasManager.GetType())}
					}
				}
			};
		}

		public static string ClrType(Type t)
		{
			return string.Concat(t.FullName, ", ", t.Assembly.GetName().Name);
		}

		private void Store(IDocumentStore store, params object[] objs)
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Advanced.UseOptimisticConcurrency = true;

				foreach (object obj in objs)
					session.Store(obj);
				session.SaveChanges();
			}
		}
	}
}