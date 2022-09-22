using System.Collections.Generic;
using System;

namespace Raven.Server.Documents.Includes;

public interface IIncludeRevisions
{
    public Dictionary<string, Document> RevisionsChangeVectorResults { get; }

    public Dictionary<string, Dictionary<DateTime, Document>> IdByRevisionsByDateTimeResults { get; }
}
