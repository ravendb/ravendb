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
			for (int i = 0; i < 150; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using (var x = new AttachmentReplication())
				{
					x.Can_replicate_delete_between_two_instances();
				}
			}
		}
	}
}
