// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.Tests.Storage.MultiThreaded
{
	public class BigPutOperation : PutOperation
	{
		protected override int SetupData()
		{
			for (int i = 1; i <= 1000; i++)
			{
				DocumentDatabase.Put("Raven/Hilo/posts" + i, null, new RavenJObject(), new RavenJObject(), null);

				for (int j = 1; j <= 49; j++)
				{
					DocumentDatabase.Put(string.Format("posts{0}/{1}", i, j), null, new RavenJObject(), new RavenJObject(), null);
				}
			}

			return 1000 * 50;
		}
	}
}