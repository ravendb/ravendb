using System;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.FileSystem;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    internal class DeleteByQueryOperation : IFilesOperation
    {
        public string FileName { get; set; }

        private string Query { get; set; }

        public DeleteByQueryOperation(string query)
        {
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentNullException("query", "The query cannot be null, empty or whitespace!");

            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentNullException("query", "The query cannot be null, empty or whitespace!");

            Query = query;
        }

        public async Task<FileHeader> Execute(IAsyncFilesSession session)
        {
            var commands = session.Commands;
            await commands.DeleteByQueryAsync(Query).ConfigureAwait(false);

            return null;
        }
    }
}
