// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class AsyncShouldThrowConcurrencyException : RavenTest
	{
		[Fact]
		public async Task InsteadOfAggregateException()
		{
			using (var server = NewRemoteDocumentStore())
			{
				var foo = new Foo {Id = 1};

				using (var session = server.OpenAsyncSession())
				{
					await session.StoreAsync(foo);
					await session.SaveChangesAsync();
				}

				using (var session = server.OpenAsyncSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					await session.StoreAsync(foo);
					await AssertAsync.Throws<ConcurrencyException>(async () => await session.SaveChangesAsync());
				}
			}
		}

		private class Foo
		{
			public int Id { get; set; }
		}
	}
}