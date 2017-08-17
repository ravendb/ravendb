using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class WaitForDatabaseRecordDeletionOperation : IServerOperation
    {
        private readonly string _databaseName;
        private readonly int _timeInSec;

        public WaitForDatabaseRecordDeletionOperation(string databaseName, int timeInSec = 0)
        {
            Helpers.AssertValidDatabaseName(databaseName);
            _databaseName = databaseName;
            _timeInSec = timeInSec;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new WaitForDatabaseRecordDeletionCommand(_databaseName, _timeInSec);
        }

        private class WaitForDatabaseRecordDeletionCommand : RavenCommand
        {
            private readonly string _databaseName;
            private readonly int _timeInSec;

            public WaitForDatabaseRecordDeletionCommand(string databaseName, int timeInSec)
            {
                if (string.IsNullOrEmpty(databaseName))
                    throw new ArgumentNullException(databaseName);
                _databaseName = databaseName;
                _timeInSec = timeInSec;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/wait/deletion?name={_databaseName}&time={_timeInSec}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }
        }
    }
}
