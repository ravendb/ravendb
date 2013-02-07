using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.MonoForAndroid.Models;

namespace Raven.Tests.MonoForAndroid
{
	public class MonoForAndroidTestBase
	{
		private const string Url = "http://10.0.0.2:8080";

		public IDocumentStore CreateDocumentStore()
		{
			return new DocumentStore { Url = Url, DefaultDatabase = "Mono" }.Initialize();
		}
	}
}