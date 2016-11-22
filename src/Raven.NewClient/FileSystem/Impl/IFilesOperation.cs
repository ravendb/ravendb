using Raven.NewClient.Abstractions.FileSystem;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Impl
{
    internal interface IFilesOperation 
    {
        string FileName { get; }

        Task<FileHeader> Execute(IAsyncFilesSession session);
    }
}
