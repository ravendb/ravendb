using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Issues;
using System.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 100; i++)
			{
				using(var x = new CompiledIndexesNhsevidence2())
				{
					x.CanGetCorrectResults();
				}
			}
		}
	}
}