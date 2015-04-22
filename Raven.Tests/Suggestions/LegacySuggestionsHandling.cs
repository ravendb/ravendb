//-----------------------------------------------------------------------
// <copyright file="Suggestions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database.Extensions;
using Raven.Tests.Bugs;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Suggestions
{
	public class LegacySuggestionsHandling : RavenTest, IDisposable
	{
		public class DefaultSuggestionIndex : AbstractIndexCreationTask<User>
		{
			public DefaultSuggestionIndex()
			{
				Map = users => from user in users
				               select new {user.Name};

				Suggestion(user => user.Name);
			}
		}

		[Fact]
		public void ShouldHandleLegacySuggestions()
		{
			var dataDir = NewDataPath();

			using (var documentStore = NewDocumentStore(runInMemory:false, dataDir: dataDir))
			{
				documentStore.ExecuteIndex(new DefaultSuggestionIndex());

				using (var s = documentStore.OpenSession())
				{
					s.Store(new User {Name = "Ayende"});
					s.Store(new User {Name = "Oren"});
					s.SaveChanges();

					s.Query<User, DefaultSuggestionIndex>().Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("DefaultSuggestionIndex", new SuggestionQuery
				{
					Field = "Name",
					Term = "Owen",
					MaxSuggestions = 10,
				});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}

			// simulate legacy suggestions by renaming folder
			var suggestionsDir = Path.Combine(dataDir, "System\\Indexes\\Raven-Suggestions\\DefaultSuggestionIndex");
			var newDirName = MonoHttpUtility.UrlEncode("Name-" + StringDistanceTypes.NGram + "-" + 0.4f);
			Directory.Move(Path.Combine(suggestionsDir, "Name"), Path.Combine(suggestionsDir, newDirName));

			using (var documentStore = NewDocumentStore(runInMemory: false, dataDir: dataDir))
			{
				var suggestionQueryResult = documentStore.DatabaseCommands.Suggest("DefaultSuggestionIndex", new SuggestionQuery
				{
					Field = "Name",
					Term = "Owen",
					MaxSuggestions = 10,
				});

				Assert.Equal(1, suggestionQueryResult.Suggestions.Length);
				Assert.Equal("oren", suggestionQueryResult.Suggestions[0]);
			}

			Assert.True(Directory.Exists(Path.Combine(suggestionsDir, "Name")));
			Assert.False(Directory.Exists(Path.Combine(suggestionsDir, newDirName)));
		}

	}
}