//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Bundles.Tests.Expiration;
using Raven.Bundles.Tests.Replication;
using Raven.Bundles.Tests.Replication.Bugs;

namespace Raven.Bundles.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			Console.WriteLine("starting...");
			using (var x = new David())
			{
				x.Can_replicate_between_two_instances_create_delete_create_quickly();
			}
		}
	}
}
