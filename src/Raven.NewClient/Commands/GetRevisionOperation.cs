using System;
using System.Collections.Generic;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class GetRevisionOperation
    {
        private static readonly Logger _logger =
            LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");

        public GetRevisionOperation()
        {
        }

        protected void LogGetRevision()
        {
            //TODO
        }

        public GetRevisionCommand CreateRequest(string key, int start, int pageSize)
        {
            return new GetRevisionCommand()
            {
                Key = key,
                Start = start,
                PageSize = pageSize
            };
        }
    }
}