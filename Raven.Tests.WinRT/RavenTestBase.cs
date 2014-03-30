// -----------------------------------------------------------------------
//  <copyright file="RavenTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Runtime.CompilerServices;
using Microsoft.VisualStudio.TestPlatform.UnitTestFramework;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Tests.WinRT
{
	[TestClass]
	public abstract class RavenTestBase
	{
		public string Url { get; private set; }

		public RavenTestBase(bool useFiddler = false)
		{
			Url = useFiddler
				      ? "http://ipv4.fiddler:8079"
				      : "http://localhost:8079";
		}

		protected static string GenerateNewDatabaseName([CallerMemberName] string memberName = "")
		{
			return memberName + "-" + SystemTime.UtcNow.Ticks;
		}

		protected IDocumentStore NewDocumentStore()
		{
			return new DocumentStore { Url = Url }.Initialize();
		}
	}
}