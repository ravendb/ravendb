// -----------------------------------------------------------------------
//  <copyright file="RecoveryTestIndex.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Indexes;

namespace Raven.Tests.Indexes.Recovery
{
	public class MapRecoveryTestIndex : AbstractIndexCreationTask<Recovery>
	{
		public MapRecoveryTestIndex()
		{
			Map = recoveries => from recovery in recoveries select new {recovery.Name, recovery.Number};
		}
	}
}