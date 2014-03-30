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

				public const string Metadata = "metadata";
			}
		}

	    public class General
	    {
	        public const string TableName = "general";
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

				public const string Data = "data";
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

		public class Tasks
		{
			public const string TableName = "tasks";

			public class Indices
			{
				public const string ByType = "by_type";

				public const string ByIndexAndType = "by_index_and_type";

				public const string ByIndex = "by_index";
			}
		}

		public class ScheduledReductions
		{
			public const string TableName = "scheduled_reductions";

			public class Indices
			{
				public const string ByView = "by_view";

				public const string ByViewAndLevelAndReduceKey  = "by_view_and_level_and_reduce_key";
			}
		}

		public class Attachments
		{
			public const string TableName = "attachments";

			public class Indices
			{
				public const string ByEtag = "key_by_etag";

                public const string Metadata = "metadata";
			}
		}

		public class MappedResults
		{
			public const string TableName = "mapped_results";

			public class Indices
			{
				public const string ByViewAndDocumentId = "by_view_and_document_id";

				public const string ByView = "by_view";

				public const string ByViewAndReduceKey = "by_view_and_reduce_key";

				public const string ByViewAndReduceKeyAndSourceBucket = "by_view_and_reduce_key_and_source_bucket";

				public const string Data = "data";
			}
		}

		public class ReduceKeyCounts
		{
			public const string TableName = "reduce_key_counts";

			public class Indices
			{
				public const string ByView = "by_view";
			}
		}

		public class ReduceKeyTypes
		{
			public const string TableName = "reduce_key_types";

			public class Indices
			{
				public const string ByView = "by_view";
			}
		}

		public class ReduceResults
		{
			public const string TableName = "reduce_results";

			public class Indices
			{
				public const string ByViewAndReduceKeyAndLevelAndSourceBucket = "by_view_and_reduce_key_and_level_and_source_bucket";

				public const string ByViewAndReduceKeyAndLevelAndBucket = "by_view_and_reduce_key_and_level_and_bucket";

				public const string ByView = "by_view";

				public const string ByViewAndReduceKeyAndLevel = "by_view_and_reduce_key_and_level";

				public const string Data = "data";
			}
		}

		public class ReduceStats
		{
			public const string TableName = "reduce_stats";
		}
	}
}