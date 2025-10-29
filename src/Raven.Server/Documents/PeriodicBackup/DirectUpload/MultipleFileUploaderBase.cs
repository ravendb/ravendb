using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public abstract class MultipleFileUploaderBase<T> : DirectFileUploader
{
    protected readonly LinkedList<T> _threads;
    protected readonly ConcurrentSet<Exception> _exceptions;

    protected MultipleFileUploaderBase(UploaderSettings settings, RetentionPolicyBaseParameters retentionPolicyParameters, RavenLogger logger, BackupResult backupResult,
        Action<IOperationProgress> onProgress, OperationCancelToken taskCancelToken) :
        base(settings, retentionPolicyParameters, logger, backupResult, onProgress, taskCancelToken)
    {
        _threads = new LinkedList<T>();
        _exceptions = new ConcurrentSet<Exception>();
    }
}
