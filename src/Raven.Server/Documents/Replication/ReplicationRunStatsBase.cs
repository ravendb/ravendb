using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication;
using Raven.Client.Util;

namespace Raven.Server.Documents.Replication
{
    public abstract class ReplicationRunStatsBase
    {
        public List<ReplicationError> Errors;

        public void AddError(Exception exception)
        {
            AddError($"Exception occurred: {exception}");
        }

        private void AddError(string message)
        {
            if (Errors == null)
                Errors = new List<ReplicationError>();

            Errors.Add(new ReplicationError
            {
                Timestamp = SystemTime.UtcNow,
                Error = message ?? string.Empty
            });
        }
    }
}