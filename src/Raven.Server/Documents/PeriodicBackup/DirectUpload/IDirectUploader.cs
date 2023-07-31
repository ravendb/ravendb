using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public interface IDirectUploader
{
    public AwsS3MultiPartUploader GetUploader(string key, Dictionary<string, string> metadata);
}
