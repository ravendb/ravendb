using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents
{
    public class DatabaseTopology
    {
        public List<string> Members = new List<string>();
        public List<string> Promotables = new List<string>();
        public List<string> Watchers = new List<string>();

        public bool RelevantFor(string nodeTag)
        {
            return Members.Contains(nodeTag) ||
                   Promotables.Contains(nodeTag) ||
                   Watchers.Contains(nodeTag);
        }

        public IEnumerable<string> AllNodes
        {
            get

            {
                foreach (var member in Members)
                {
                    yield return member;
                }
                foreach (var member in Promotables)
                {
                    yield return member;
                }
                foreach (var member in Watchers)
                {
                    yield return member;
                }
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables),
                [nameof(Watchers)] = new DynamicJsonArray(Watchers)
            };
        }

        public void RemoveFromTopology(string delDbFromNode)
        {
            Members.Remove(delDbFromNode);
            Promotables.Remove(delDbFromNode);
            Watchers.Remove(delDbFromNode);
        }
    }
}
