using System;
using System.Collections.Generic;
using System.Diagnostics;
<<<<<<< HEAD
using Raven.Abstractions.Counters;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Tests.Core;
using Raven.Tests.Core.BulkInsert;
=======
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database.Storage.Voron.Impl;
>>>>>>> 799bb2f8945ce9571d7008db2de0c23fdc0fe1a7

namespace ConsoleApplication4
{
<<<<<<< HEAD
	public class Program
	{
		public static void Main()
		{			
			for (int i = 0; i < 1000; i++)
			{
				try
				{
					//if(i % 100 == 0)
						Console.WriteLine(i);
					using (var f = new TestServerFixture())
					{
						var test = new ChunkedBulkInsert();
						test.SetFixture(f);
						test.ValidateChunkedBulkInsertOperationsIDsCount();
					}
				}
				catch (Exception e)
				{
					Debugger.Break();
				}
			}
		}
	}
=======
    class Program
    {
        public class Item
        {
            public int Number;
        }
        private static void Main(string[] args)
        {
            var ds = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "mr"
            }.Initialize();

            using (var bulk = ds.BulkInsert())
            {
                for (int i = 0; i < 1000 * 1000; i++)
                {
                    bulk.Store(new Item { Number = 1 });
                }
            }

        }

    }

    public class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }

        public string Phone { get; set; }
        public string Fax { get; set; }
    }
>>>>>>> 799bb2f8945ce9571d7008db2de0c23fdc0fe1a7
}
