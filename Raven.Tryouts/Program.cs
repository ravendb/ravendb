using System;
using System.IO;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Database;
using Raven.Database.Config;
using Raven.Scenarios;
using Raven.Storage.Esent;
using Raven.Tests.Indexes;
using Raven.Tests.Triggers;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
            DocumentDatabase.Restore(new RavenConfiguration
            {
                DefaultStorageTypeName = typeof(TransactionalStorage).AssemblyQualifiedName
            }, @"C:\Users\Ayende\Downloads\Backup\Backup", @"C:\Users\Ayende\Downloads\Backup\DB");
		}
	}
}
