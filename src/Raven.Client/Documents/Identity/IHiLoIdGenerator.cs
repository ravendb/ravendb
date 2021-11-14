using System;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Identity
{
    public interface IHiLoIdGenerator
    {
        Task<long> GenerateNextIdForAsync(string database, object entity);

        Task<long> GenerateNextIdForAsync(string database, Type type);

        Task<long> GenerateNextIdForAsync(string database, string collectionName);
    }
}
