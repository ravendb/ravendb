using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerOptions
    {
        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Documents | DatabaseItemType.Transformers;
        }

        public long? StartDocsEtag { get; set; }

        public DatabaseItemType OperateOnTypes { get; set; }

        public bool IgnoreErrorsAndContinue { get; set; }

        public long? Limit { get; set; }

        public string TransformScript { get; set; }

        public bool SkipConflicted { get; set; }

        public bool StripReplicationInformation { get; set; }

        public int MaxStepsForTransformScript { get; set; }

        public string Database { get; set; }
    }
}