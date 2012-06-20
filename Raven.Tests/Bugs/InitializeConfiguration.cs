// -----------------------------------------------------------------------
//  <copyright file="InitializeConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class CanCustomizeConventionsBeforeInitializingTheStore : IDisposable
	{
		private readonly DocumentStore store;

		public CanCustomizeConventionsBeforeInitializingTheStore()
		{
			store = new DocumentStore {Url = "http://localhost:7079"};
		}


		public void Dispose()
		{
			store.Dispose();
		}

		[Fact]
		public void DocumentKeyGenerator()
		{
			var generator = new MultiTypeHiLoKeyGenerator(5);
			store.Conventions.DocumentKeyGenerator = (cmd, entity) => generator.GenerateDocumentKey(cmd, store.Conventions, entity);
			store.Initialize();
		}
	}
}