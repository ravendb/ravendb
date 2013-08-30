// -----------------------------------------------------------------------
//  <copyright file="Tables.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron
{
	public class Tables
	{
		public class Details
		{
			public const string TableName = "details";
		}

		public class Documents
		{
			public const string TableName = "documents";

			public class Indices
			{
				public const string KeyByEtag = "key_by_etag";
			}
		}
	}
}