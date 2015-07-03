// -----------------------------------------------------------------------
//  <copyright file="DiskSpaceNotification.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Data
{
	public class DiskSpaceNotification
	{
		public string Path { get; private set; }

		public PathType PathType { get; private set; }

		public double FreeSpaceInPercentage { get; private set; }

		public DiskSpaceNotification(string path, PathType pathType, double freeSpaceInPercentage)
		{
			Path = path;
			PathType = pathType;
			FreeSpaceInPercentage = freeSpaceInPercentage;
		}
	}

	public enum PathType
	{
		Data,
		Index,
		Journal
	}
}