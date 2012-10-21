using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Tests.Bugs;
using System.Linq;
using Raven.Tests.Faceted;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			var x = new DynamicViewCompiler("test", new IndexDefinition
			{
				Map = "from doc in docs select new { doc.Name }"
			}, ".");
			x.GenerateInstance();
		}
	}
}