// -----------------------------------------------------------------------
//  <copyright file="RavenTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Document;

namespace Raven.Tests.Core
{
	public class RavenTestBase
	{
		protected string Url
		{
			get
			{
				if (UseFiddler)
					return "http://ipv4.fiddler:8079";
				return "http://localhost:8079";
			}
		}

		protected bool UseFiddler { get; set; }

		protected static string GenerateNewDatabaseName()
		{
			var stackTrace = new StackTrace();
			var stackFrame = stackTrace.GetFrames()
			                           .First(x => x.GetMethod().Name == "MoveNext" &&
			                                       x.GetMethod().DeclaringType.FullName.Contains("+<"));

			var generateNewDatabaseName = stackFrame.GetMethod().DeclaringType.FullName.Replace("+<", ".");

			return generateNewDatabaseName
				.Substring(0, generateNewDatabaseName.IndexOf(">"))
				.Replace("Raven.Tests.Core.", string.Empty) + SystemTime.UtcNow.Ticks;
		}

		protected IDocumentStore NewDocumentStore()
		{
			return new DocumentStore { Url = Url }.Initialize();
		}
	}
}