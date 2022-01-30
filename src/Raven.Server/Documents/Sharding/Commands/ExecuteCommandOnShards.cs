using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Commands
{
    public class ExecuteCommandOnShardsResult<TResult> : IDisposable
    {
        private readonly List<IDisposable> _disposables;
        public List<ShardCommandResult<TResult>> ShardCommandResults;
        public ExecuteCommandOnShardsResult()
        {
            _disposables = new List<IDisposable>();
        }

        public void AddDisposable(IDisposable disposable)
        {
            _disposables.Add(disposable);
        }

        public void Dispose()
        {
            _disposables.ForEach(x => x.Dispose());
        }
    }

    public class ShardCommandResult<TResult>
    {
        public RavenCommand<TResult> Command;
        public string Shard;
        public Task ExecuteTask;
    }

}
