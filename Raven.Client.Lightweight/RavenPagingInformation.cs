// -----------------------------------------------------------------------
//  <copyright file="RavenPagingInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Client
{
    public class RavenPagingInformation
    {
        private int previousNextPageStart;

        public int Start { get; private set; }

        public int PageSize { get; private set; }

        public int NextPageStart { get; private set; }

        public void Fill(int start, int pageSize, int nextPageStart)
        {
            if (start < 0)
                throw new InvalidOperationException("Start must be greater or equal than 0.");

            if (pageSize <= 0)
                throw new InvalidOperationException("PageSize must be greater than 0.");

            Start = start;
            PageSize = pageSize;
            previousNextPageStart = NextPageStart;
            NextPageStart = nextPageStart;
        }

        public bool IsForPreviousPage(int start, int pageSize)
        {
            if (PageSize != pageSize)
                return false;

            return IsLastPage() == false && Start + PageSize == start;
        }

        public bool IsLastPage()
        {
            return previousNextPageStart == NextPageStart;
        }
    }
}
