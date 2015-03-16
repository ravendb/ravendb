using System;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;

namespace Raven.Client.FileSystem.Impl
{
	internal class DeleteByQueryOperation : IFilesOperation
    {
		public string FileName { get; set; }

		private string Query { get; set; }
		private string[] OrderByFields { get; set; }
		private int Start { get; set; }
		private int PageSize { get; set; }

		public DeleteByQueryOperation(string query, string[] orderByFields, int start, int pageSize)
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentNullException("query", "The query cannot be null, empty or whitespace!");

			if (string.IsNullOrWhiteSpace(query))
				throw new ArgumentNullException("query", "The query cannot be null, empty or whitespace!");

			Query = query;
			OrderByFields = orderByFields;
			Start = start;
			PageSize = pageSize;
        }

		public async Task<FileHeader> Execute(IAsyncFilesSession session)
	    {
		    var commands = session.Commands;
            await commands.DeleteByQueryAsync(Query, OrderByFields, Start, PageSize).ConfigureAwait(false);

		    return null;
	    }
    }
}