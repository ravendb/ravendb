using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Shard
{
    /// <summary>
    /// Apply an operation to all the shard session in sequence
    /// </summary>
    public class SequentialShardAccessStrategy : IShardAccessStrategy
    {
        private readonly Task runFirst;

        public SequentialShardAccessStrategy(IDictionary<string, IAsyncFilesCommands> shards)
        {
            this.runFirst = ValidateShards(shards);
        }

        private async Task ValidateShards(IEnumerable<KeyValuePair<string, IAsyncFilesCommands>> shards)
        {
            var shardsKeyIdList = new List<Tuple<string, Guid>>();
            foreach (var shard in shards)
            {
                try
                {
                    var id = await shard.Value.GetServerIdAsync().ConfigureAwait(false);
                    shardsKeyIdList.Add(Tuple.Create(shard.Key, id));
                }
                catch (Exception)
                {
                    // ignore the error here
                }
            }

            var shardsPointingToSameDb = shardsKeyIdList
                .GroupBy(x => x.Item2)
                .FirstOrDefault(x => x.Count() > 1);

            if (shardsPointingToSameDb != null)
                throw new NotSupportedException(string.Format("Multiple keys in shard dictionary for {0} are not supported.",
                    string.Join(", ", shardsPointingToSameDb.Select(x => x.Item1))));
        }

        public event ShardingErrorHandle<IAsyncFilesCommands> OnAsyncError;

        public async Task<T[]> ApplyAsync<T>(IList<IAsyncFilesCommands> commands, ShardRequestData request, Func<IAsyncFilesCommands, int, Task<T>> operation)
        {
            await runFirst.ConfigureAwait(false);

            var list = new List<T>();
            var errors = new List<Exception>();
            for (int i = 0; i < commands.Count; i++)
            {
                try
                {
                    list.Add(await operation(commands[i], i).ConfigureAwait(false));
                }
                catch (Exception e)
                {
                    var error = OnAsyncError;
                    if (error == null)
                        throw;
                    if (error(commands[i], request, e) == false)
                    {
                        throw;
                    }
                    errors.Add(e);
                }
            }

            // if ALL nodes failed, we still throw
            if (errors.Count == commands.Count)
                throw new AggregateException(errors);

            return list.ToArray();
        }
    }
}
