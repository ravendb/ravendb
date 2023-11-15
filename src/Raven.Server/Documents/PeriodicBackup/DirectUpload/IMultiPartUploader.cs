using System.IO;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public interface IMultiPartUploader
{
    void Initialize();

    Task InitializeAsync();

    void UploadPart(Stream stream, long size);

    Task UploadPartAsync(Stream stream, long size);

    void CompleteUpload();

    Task CompleteUploadAsync();
}
