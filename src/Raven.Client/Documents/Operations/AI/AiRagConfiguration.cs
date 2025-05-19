
using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.AI;

public class AiRagConfiguration
{
    public string ConnectionStringName { get; set; }
    public string SystemPrompt { get; set; }
    public string OutputSchema { get; set; }
    public List<ToolQuery> Queries { get; set; }= [];
    public List<ToolAction> Actions { get; set; } = [];
    public PersistenceConfiguration Persistence { get; set; }

    public class PersistenceConfiguration
    {
        public string Collection { get; set; }
        public TimeSpan? Expires { get; set; }
    }
    
    public class ToolAction
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string ParametersSchema { get; set; }
    }
    public class ToolQuery
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public string Query { get; set; }
        
        public string ParametersSchema { get; set; }
    }

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
}
