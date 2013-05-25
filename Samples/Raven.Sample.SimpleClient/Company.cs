//-----------------------------------------------------------------------
// <copyright file="Company.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Shard;
using Raven.Client;

namespace Raven.Sample.SimpleClient
{
	public class Company
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string Region { get; set; }
	}
}
