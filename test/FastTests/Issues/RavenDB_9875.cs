using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_9875 : NoDisposalNeeded
    {
        public RavenDB_9875(ITestOutputHelper output) : base(output)
        {
        }

        private readonly HashSet<Type> _willNotUseTheCacheOutsideItsScopeBecauseWeDoubleCheckedThat
            = new HashSet<Type>
            {
                typeof(GetDocumentsCommand),
                typeof(QueryCommand),
                typeof(GetRevisionsBinEntryCommand),
                typeof(GetRevisionsCommand),
                typeof(SingleNodeBatchCommand),
                typeof(ClusterWideBatchCommand),
                typeof(PatchOperation.PatchCommand),
                typeof(GetTimeSeriesOperation<>.GetTimeSeriesCommand),
                typeof(ConditionalGetDocumentsCommand)
            };

        [Fact]
        public void Commands_should_be_careful_about_blittable_usage()
        {
            var commandTypes = typeof(RavenCommand<>).Assembly.GetTypes().Where(t =>
            {
                while (t != typeof(object) && t != null && t.BaseType != null)
                {
                    if (t.BaseType.IsGenericType)
                    {
                        if (t.BaseType.GetGenericTypeDefinition() == typeof(RavenCommand<>))
                            return true;
                    }

                    t = t.BaseType;
                }

                return false;
            }).ToList();
            var sb = new StringBuilder();
            foreach (var type in commandTypes)
            {
                var t = type;
                while (t.BaseType.IsGenericType == false)
                {
                    t = t.BaseType;
                }
                var arg = t.BaseType.GetGenericArguments()[0];
                foreach (var item in arg.GetProperties())
                {
                    if (item.PropertyType == typeof(BlittableJsonReaderObject) ||
                        item.PropertyType == typeof(BlittableJsonReaderArray)
                        )
                    {
                        if (_willNotUseTheCacheOutsideItsScopeBecauseWeDoubleCheckedThat.Contains(type) == false)
                        {
                            sb.AppendLine("The type " + type.FullName + " has property " + item.Name + " of type " + item.PropertyType.FullName + " and didn't validate that is isn't copying the cached value correctly");
                        }
                    }
                }
                foreach (var item in arg.GetFields())
                {
                    if (item.FieldType == typeof(BlittableJsonReaderObject) ||
                        item.FieldType == typeof(BlittableJsonReaderArray)
                        )
                    {
                        if (_willNotUseTheCacheOutsideItsScopeBecauseWeDoubleCheckedThat.Contains(type) == false)
                        {
                            sb.AppendLine("The type " + type.FullName + " has field " + item.Name + " of type " + item.FieldType.FullName + " and didn't validate that is isn't copying the cached value correctly");
                        }
                    }
                }
            }


            if (sb.Length > 0)
                throw new InvalidOperationException(sb.ToString());
        }
    }
}
