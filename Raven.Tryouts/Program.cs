using System;
using System.IO;
using System.Xml;
using NLog;
using NLog.Config;
using Raven.Database.Server;
using Raven.Tests.Bugs;
using Raven.Tests.Bugs.Caching;
using Raven.Tests.Shard.BlogModel;
using Raven.Tests.Queries;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			new IntersectionQuery().CanPeformIntersectionQuery_Embedded();
			new IntersectionQuery().CanPerformIntersectionQuery_Remotely();
		}
	}
}