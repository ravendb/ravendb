// -----------------------------------------------------------------------
//  <copyright file="Tables.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
	public class Tables
	{
		public class Details
		{
			public const string TableName = "details";

		    public static string Key = "details";
		}

		public class Pages
		{
			public const string TableName = "pages";

		    public class Indices
		    {
                public const string ByKey = "by_key";
		    }
		}

	    public class Usage
	    {
	        public const string TableName = "usage";

	        public class Indices
	        {
                public const string ByFileNameAndPosition = "by_file_name_and_position";
	        }
	    }

		public class Config
		{
			public const string TableName = "config";
		}

		public class Signatures
		{
            public const string TableName = "signatures";
		}

        public class Files
		{
            public const string TableName = "files";
		}
	}
}