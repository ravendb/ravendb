using System.Threading.Tasks;

namespace Raven.Client.Documents.Identity
{
    public interface IHiLoIdGenerator
    {
        Task<string> GenerateDocumentIdAsync(string database, object entity);

        Task<long> GenerateNextIdForAsync(string database, string tag);
    }
}
