using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public interface IDirectUploader : IDisposable
{
    public IMultiPartUploader GetUploader(string key, Dictionary<string, string> metadata);
}
