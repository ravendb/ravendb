// -----------------------------------------------------------------------
//  <copyright file="MultiThreadedStorages.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;

namespace Raven.StressTests.Storage.MultiThreaded
{
	public class PutOperation : MultiThreaded
	{
		protected override int SetupData()
		{
			DocumentDatabase.Put("Raven/Hilo/users", null, new RavenJObject(), new RavenJObject(), null);
			DocumentDatabase.Put("Raven/Hilo/posts", null, new RavenJObject(), new RavenJObject(), null);

			return 2;
		}
	}

	public class MediumPutOperation : PutOperation
	{
		protected override int SetupData()
		{
			for (int i = 1; i <= 10; i++)
			{
				DocumentDatabase.Put("Raven/Hilo/posts" + i, null, new RavenJObject(), new RavenJObject(), null);

				for (int j = 1; j <= 6; j++)
				{
					DocumentDatabase.Put(string.Format("posts{0}/{1}", i, j), null, new RavenJObject(), new RavenJObject(), null);
				}
			}

			return 10 * 7;
		}
	}

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