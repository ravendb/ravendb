using System.IO;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public interface IMultiPartUploader
{
    void Initialize();

    Task InitializeAsync();

    void UploadPart(Stream stream);

    Task UploadPartAsync(Stream stream);

    void CompleteUpload();

    Task CompleteUploadAsync();
}
