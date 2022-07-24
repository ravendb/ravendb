using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Backups
{
    public class ShardRestoreSetting : IDynamicJson
    {
        public int ShardNumber { get; set; }
        public string NodeTag { get; set; }
        public string BackupPath { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ShardNumber)] = ShardNumber,
                [nameof(NodeTag)] = NodeTag,
                [nameof(BackupPath)] = BackupPath
            };
        }
    }
}
