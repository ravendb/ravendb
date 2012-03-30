using System;
using System.IO;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Bundles.Tests.Authorization.Bugs;
using Raven.Bundles.Tests.Replication;
using Raven.Database.Server;
using Raven.StressTests.Races;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			new BundelsRaceConditions().FailoverBetweenTwoMultiTenantDatabases_CanReplicateBetweenTwoMultiTenantDatabases();
		}
	}
}