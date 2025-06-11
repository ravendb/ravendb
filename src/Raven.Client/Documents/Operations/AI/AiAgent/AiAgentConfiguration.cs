using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.AiAgent;

public class AiAgentConfiguration : IDynamicJson
{
    public AiAgentConfiguration()
    {
        // for serialization purposes
    }

    public AiAgentConfiguration(string connectionStringName, string systemPrompt)
    {
        ConnectionStringName = connectionStringName ?? throw new ArgumentNullException(nameof(connectionStringName));
        SystemPrompt = systemPrompt ?? throw new ArgumentNullException(nameof(systemPrompt));
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

    /*
    public List<string> FindParameters()
    {
        foreach (ToolQuery query in Queries ?? [])
        {
            if(query.Name == name)
                return query;
        }

        return null;
    }*/

    public ToolQuery FindQuery(string name)
    {
        foreach (ToolQuery query in Queries ?? [])
        {
            if(query.Name == name)
                return query;
        }

        return null;
    }
    
    public ToolAction FindAction(string name)
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
