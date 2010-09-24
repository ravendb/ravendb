using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Database.Data;

namespace Raven.Bundles.DynamicQueries.Data
{
    public class DynamicQuery
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicQuery"/> class.
		/// </summary>
        public DynamicQuery()
        {
            TotalSize = new Reference<int>();
            PageSize = 128;
        }

		/// <summary>
		/// Gets or sets the query.
		/// </summary>
		/// <value>The query.</value>
        public string Query { get; set; }

        /// <summary>
        /// Gets or sets the mappings (This might be temporary, better solutions are incoming)
        /// </summary>
        public DynamicQueryMap[] Mappings { get; set; }

		/// <summary>
		/// Gets or sets the total size.
		/// </summary>
		/// <value>The total size.</value>
        public Reference<int> TotalSize { get; private set; }

		/// <summary>
		/// Gets or sets the start of records to read.
		/// </summary>
		/// <value>The start.</value>
        public int Start { get; set; }

		/// <summary>
		/// Gets or sets the size of the page.
		/// </summary>
		/// <value>The size of the page.</value>
        public int PageSize { get; set; }

		/// <summary>
		/// Gets or sets the fields to fetch.
		/// </summary>
		/// <value>The fields to fetch.</value>
        public string[] FieldsToFetch { get; set; }
    }
}
