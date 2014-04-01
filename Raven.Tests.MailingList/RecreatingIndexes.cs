// -----------------------------------------------------------------------
//  <copyright file="RecreatingIndexes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Actions;
using Raven.Database.Storage;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class RecreatingIndexes : RavenTest
	{
		public class Audio
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class Audio_Index : AbstractIndexCreationTask<Audio>
		{
			public Audio_Index()
			{
				// audio properties
				Map = audios => from audio in audios
								select new
								{
									Id = audio.Id,
									AudioId = audio.Id,
									Name = audio.Name
								};

				Analyzers.Add(x => x.Name, "SimpleAnalyzer");
			}
		}

		[Fact]
		public void ShouldNoRecreate()
		{
			using(var store = NewDocumentStore())
			{
				new Audio_Index().Execute(store);

				var index = new Audio_Index
				{
					Conventions = new DocumentConvention()
				}.CreateIndexDefinition();
				var findIndexCreationOptions = typeof(IndexActions).GetMethod("FindIndexCreationOptions", BindingFlags.Instance | BindingFlags.NonPublic);
				var result = findIndexCreationOptions.Invoke(store.DocumentDatabase.Indexes, new object[] { index, "Audio/Index" });
				Assert.Equal(IndexCreationOptions.Noop, result);
			}
		}
	}
}