using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization;
using System.Xml;
using Raven.Client;
using Raven.Client.Document;
using Raven.StressTests.Races;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main(string[] args)
		{
			new LazyStressed().LazyMultiLoadOperationWouldBeInTheSession_WithNonStaleResponse();
		}
	}
}