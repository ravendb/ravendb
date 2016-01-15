// -----------------------------------------------------------------------
//  <copyright file="FilesQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Client.FileSystem
{
    public class FilesQuery
    {
        public FilesQuery(string query, int start, int? pageSize, string[] sortFields)
        {
            Query = query;
            Start = start;

            if (pageSize.HasValue)
            {
                PageSizeSet = true;
                PageSize = pageSize.Value;
            }

            SortFields = sortFields;
        }

        public string Query { get; set; }

        public int Start { get; private set; }

        public bool PageSizeSet { get; private set; }

        public int PageSize { get; private set; }

        public string[] SortFields { get; private set; }
    }
}