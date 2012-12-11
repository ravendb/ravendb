using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
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
using Raven.Tests.Document;
using Raven.Tests.Faceted;
using Raven.Tests.Issues;
using System.Linq;
using Raven.Tests.Util;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				Console.WriteLine(i);
				using (var x = new FacetedIndex())
				{
					x.CanPerformFacetedSearch_Remotely_Lazy_can_work_with_others();
				}
			}
		}
	}
}