using System;
using System.Collections.Generic;
using System.Threading;
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
using Raven.Tests.Issues;
using System.Linq;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
			var x = RavenJObject.Parse(@"{
  '_': null,
  'SaleId': 'sales/1',
  'Locations': [
    {
      'Lat': 37.78,
      'Lng': 144.96
    },
    {
      'Lat': 37.79,
      'Lng': 144.96
    }
  ],
  'TotalSold': 0,
  '__document_id': 'sales/1'
}");
			var index = new Index_Sales_2fByLocation();
			foreach (var VARIABLE in index.ReduceDefinition(new[]{new DynamicJsonObject(x)}))
			{
				Console.WriteLine(VARIABLE);
			}
		}
	}
}