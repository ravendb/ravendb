using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class AvoidRaceConditionWhenWeLoadTheDataNotPatched : RavenTest
	{
		[Fact]
		public void GetReturnsFilteredResults()
		{
			using (var store = NewDocumentStore())
			{
				var users = new[]
				{
					new UserProfile {Id = "users/1"},
					new UserProfile {Id = "users/2"},
					new UserProfile {Id = "users/3"}
				};

				var innovations = new[]
				{
					new InnovationProfile {Id = "innovations/1"},
					new InnovationProfile {Id = "innovations/2"},
					new InnovationProfile {Id = "innovations/3"},
					new InnovationProfile {Id = "innovations/4"},
					new InnovationProfile {Id = "innovations/5"},
				};

				store.ExecuteIndex(new UserProfileIndex());
				store.RegisterListener(new NoStaleQueriesAllowed());

				using (var session = store.OpenSession())
				{
					foreach (var obj in innovations)
						session.Store(obj);
					foreach (var obj1 in users)
						session.Store(obj1);
					session.SaveChanges();
				}

				var patches = users.SelectMany(
					user => innovations.SelectMany(
						inno => Establish(inno, user)));
				Commit(patches.ToArray(), store);

				using (var session = store.OpenSession())
				{
					var user = session.Load<UserProfile>("users/1");

					if (!user.Relations.Any())
					{
						throw new Exception("User has no relations");
					}
				}
			}
		}

		public IEnumerable<ScriptedPatchCommandData> Establish(InnovationProfile edge1Profile, UserProfile edgeNProfile)
		{
			Relation edge1 = new HasCaseManager(edgeNProfile);
			Relation edgeN = new CaseManagerOf(edge1Profile);

			yield return new ScriptedPatchCommandData
			{
				Key = edge1Profile.Id,
				Patch = new ScriptedPatchRequest
				{
					Script = "addRelation(relationClrType, relation);",
					Values = new Dictionary<string, object>
					{
						{"relation", edge1},
						{"relationClrType", ClrType(edge1.GetType())},
						{"otherSideRelationType", edgeN.Type},
						{"otherSideRelationSubType", edgeN.SubType}
					}
				}
			};

			yield return new ScriptedPatchCommandData
			{
				Key = edgeNProfile.Id,
				Patch = new ScriptedPatchRequest
				{
					Script = "addRelation(relationClrType, relation);",
					Values = new Dictionary<string, object>
					{
						{"relation", edgeN},
						{"relationClrType", ClrType(edgeN.GetType())}
					}
				}
			};
		}

		public void Commit(ScriptedPatchCommandData[] patches, IDocumentStore store)
		{
			var mergedPatchCommands = patches
				.GroupBy(cmd => cmd.Key)
				.Select(g =>
				{
					string docKey = g.Select(it => it.Key).First();

					int index = 0;
					foreach (var cmd in g)
					{
						// rename parameters
						var originalValues = cmd.Patch.Values;
						cmd.Patch.Values = new Dictionary<string, object>();
						foreach (var kvValue in originalValues)
						{
							string newKey = kvValue.Key + index;
							cmd.Patch.Values.Add(newKey, kvValue.Value);
							cmd.Patch.Script = Regex.Replace(cmd.Patch.Script, @"\b" + kvValue.Key + @"\b", newKey);
						}
						index++;
					}

					var scriptedPatchRequest = new ScriptedPatchRequest
					{
						Script = "var _this = this;" +
						         "function addRelation(clrType, relation, thisArg) {" +
						         "   (thisArg || _this).Relations.push(_.extend({ '$type': clrType }, relation));" +
						         "}" +
						         string.Join("\n\n", g.Select(cmd => cmd.Patch.Script)),
						Values = g.SelectMany(cmd => cmd.Patch.Values).ToDictionary(kv => kv.Key, kv => kv.Value)
					};

					return new ScriptedPatchCommandData
					{
						Key = docKey,
						Patch = scriptedPatchRequest
					};
				})
				.ToArray();

			BatchResult[] results = store.DatabaseCommands.Batch(mergedPatchCommands);
			if (results.Any(r => r.PatchResult.Value != PatchResult.Patched))
				throw new InvalidOperationException("Some patches failed");
		}

		public static string ClrType(Type t)
		{
			return string.Concat(t.FullName, ", ", t.Assembly.GetName().Name);
		}

		public class UserProfileIndex : AbstractIndexCreationTask<UserProfile, UserProfileIndex.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public CredentialsStatus Status { get; set; }
				public IEnumerable<string> ManagedItems { get; set; }
			}

			public UserProfileIndex()
			{
				Map = docs =>
					from doc in docs
					select new Result
					{
						Id = doc.Id,
						Status = LoadDocument<Credentials>(doc.Id + "/credentials").Status,
						ManagedItems =
							from r in doc.Relations
							where r.Type == "CaseManagerOf"
							let profile = LoadDocument<InnovationProfile>(r.To.Id)
							select r.To.Id
					};
			}
		}

		public class Profile
		{
			public string Id { get; set; }

			public virtual string Name { get; set; }

			public List<Relation> Relations { get; protected set; }

			protected Profile()
			{
				Relations = new List<Relation>();
			}
		}

		public abstract class Relation
		{
			public string Type { get; protected internal set; }
			public string SubType { get; protected internal set; }

			public Profile To { get; private set; }

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
		}

		public sealed class CaseManagerOf : Relation
		{
			internal CaseManagerOf(InnovationProfile innovation)
				: base(innovation)
			{ }

			private CaseManagerOf() { }
		}

		public sealed class HasCaseManager : Relation
		{
			internal HasCaseManager(UserProfile user)
				: base(user)
			{ }

			private HasCaseManager() { }
		}

		public class InnovationProfile : Profile { }

		public class UserProfile : Profile { }

		public enum CredentialsStatus { Inactive }

		public class Credentials
		{
			public string Id { get; set; }
			public CredentialsStatus Status { get; set; }
		}

		public class NoStaleQueriesAllowed : IDocumentQueryListener
		{
			public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
			{
				queryCustomization.WaitForNonStaleResults(TimeSpan.FromSeconds(60));
			}
		}
	}
}