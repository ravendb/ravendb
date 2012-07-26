//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Bundles.Tests.IndexReplicationToRedis;

namespace Raven.Bundles.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			
			Console.WriteLine("starting...");

			using (var x = new ReplicateToRedis())
			{
				x.Can_replicate_to_redis_using_pocoTypeMode();
				x.Can_replicate_to_redis_when_document_is_updated_using_pocoTypeMode();

				x.Can_replicate_to_redis_using_hashMode();
				x.Can_replicate_to_redis_when_document_is_updated_using_hashMode();
			}
		}
	}
}
