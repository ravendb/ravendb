using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class AiAgentConfiguration : IDynamicJson
{
    public AiAgentConfiguration()
    {
        // for serialization purposes
    }

    public AiAgentConfiguration(string connectionStringName, string systemPrompt)
    {
        ValidationMethods.AssertNotNullOrEmpty(connectionStringName, nameof(connectionStringName));
        ValidationMethods.AssertNotNullOrEmpty(systemPrompt, nameof(systemPrompt));
        
        ConnectionStringName = connectionStringName;
        SystemPrompt = systemPrompt;
    }

    public string ConnectionStringName { get; set; }
    public string SystemPrompt { get; set; }
    public string OutputSchema { get; set; }
    public List<ToolQuery> Queries { get; set; }= [];
    public List<ToolAction> Actions { get; set; } = [];
    public PersistenceConfiguration Persistence { get; set; }

    public class PersistenceConfiguration : IDynamicJson
    {
        public string Collection { get; set; }
        public TimeSpan? Expires { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Collection)] = Collection,
                [nameof(Expires)] = Expires?.TotalMilliseconds
            };
        }
    }
    
    public class ToolAction : IDynamicJson
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string ParametersSchema { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }
    public class ToolQuery : IDynamicJson
    {
        public static ToolQuery Build<T>(string name, string description, IRavenQueryable<T> query)
        {
            var dq = (AsyncDocumentQuery<T>)query.ToAsyncDocumentQuery();
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                return new ToolQuery
                {
                    Name = name,
                    Description = description,
                    Query = dq.ToString(),
                    ParametersSchema = context.ReadObject(DynamicJsonValue.Convert(dq.QueryParameters), "params").ToString()
                };
            }
        }

        public string Name { get; set; }
        public string Description { get; set; }
        public string Query { get; set; }
        public string ParametersSchema { get; set; }
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Description)] = Description,
                [nameof(Query)] = Query,
                [nameof(ParametersSchema)] = ParametersSchema
            };
        }
    }

    internal ToolQuery FindQuery(string name)
    {
        foreach (ToolQuery query in Queries ?? [])
        {
            if(query.Name == name)
                return query;
        }

        return null;
    }
    internal ToolAction FindAction(string name)
    {
        foreach (ToolAction action in Actions ?? [])
        {
            if(action.Name == name)
                return action;
        }

        return null;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(SystemPrompt)] = SystemPrompt,
            [nameof(OutputSchema)] = OutputSchema,
            [nameof(Queries)] = Queries != null ? new DynamicJsonArray(Queries) : null,
            [nameof(Actions)] = Actions != null ? new DynamicJsonArray(Actions) : null,
            [nameof(Persistence)] = Persistence?.ToJson()
        };
    }
}
