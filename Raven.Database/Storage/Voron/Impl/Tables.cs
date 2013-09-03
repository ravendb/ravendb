// -----------------------------------------------------------------------
//  <copyright file="Tables.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Storage.Voron.Impl
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

		public class IndexingStats
		{
			public const string TableName = "indexing_stats";
		}

		public class LastIndexedEtags
		{
			public const string TableName = "last_indexed_etags";
		}

		public class DocumentReferences
		{
			public const string TableName = "document_references";

			public class Indices
			{
				public const string ByViewAndKey = "by_view_and_key";

				public const string ByRef = "by_ref";

				public const string ByView = "by_view";

				public const string ByKey = "by_key";
			}
		}

		public class Queues
		{
			public const string TableName = "queues";

			public class Indices
			{
				public const string ByName = "by_name";
			}
		}

		public class Lists
		{
			public const string TableName = "lists";

			public class Indices
			{
				public const string ByName = "by_name";

				public const string ByNameAndKey = "by_name_and_key";
			}
		}
	}
}