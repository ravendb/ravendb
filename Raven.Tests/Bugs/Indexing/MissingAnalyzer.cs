//-----------------------------------------------------------------------
// <copyright file="MissingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class MissingAnalyzer : RavenTest
	{
		[Fact]
		public void Should_give_clear_error_when_starting()
		{
			using (var store = NewDocumentStore())
			{
				var e = Assert.Throws<IndexCompilationException>(() => store.DatabaseCommands.PutIndex("foo",
																							   new IndexDefinition
																							   {
																								   Map =
																									   "from doc in docs select new { doc.Name}",
																								   Analyzers =
																									   {
																										   {
																											   "Name",
																											   "foo bar"
																											   }
																									   }
																							   }));

				Assert.Equal("Could not create analyzer for field: 'Name' because the type 'foo bar' was not found", e.Message);
			}
		}
	}
}
