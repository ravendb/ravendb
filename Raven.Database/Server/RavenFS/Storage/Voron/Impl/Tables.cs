// -----------------------------------------------------------------------
//  <copyright file="Tables.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
	public class Tables
	{
		public class Details
		{
			public const string TableName = "details";

		    public const string Id = "id";
		}

		public class Pages
		{
			public const string TableName = "pages";

		    public class Indices
		    {
                public const string ByKey = "by_key";

		        public const string Data = "data";
		    }
		}

	    public class Usage
	    {
	        public const string TableName = "usage";

	        public class Indices
	        {
                public const string ByFileNameAndPosition = "by_file_name_and_position";

	            public static string ByFileName = "by_file_name";
	        }
	    }

		public class Config
		{
			public const string TableName = "config";
		}

		public class Signatures
		{
            public const string TableName = "signatures";

		    public class Indices
		    {
                public static string ByName = "by_name";

		        public static string Data = "data";
		    }
		}

        public class Files
		{
            public const string TableName = "files";

            public class Indices
            {
                public const string ByEtag = "by_etag";

                public const string Count = "count";
            }
		}

	    public class FileTombstones
	    {
            public const string TableName = "file_tombstones";
	    }
	}
}