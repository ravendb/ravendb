using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Http
{
    public class ClusterTopology
    {
        public ClusterTopology(string topologyId, string apiKey, Dictionary<string, string> members, Dictionary<string, string> promotables, Dictionary<string, string> watchers, string lastNodeId)
        {
            TopologyId = topologyId;
            ApiKey = apiKey;
            Members = members;
            Promotables = promotables;
            Watchers = watchers;
            LastNodeId = lastNodeId;
        }

        public bool Contains(string node)
        {
            return Members.ContainsKey(node) || Promotables.ContainsKey(node) || Watchers.ContainsKey(node);
        }

        //Try to avoid using this since it is expensive
        public (bool hasUrl,string nodeTag) TryGetNodeTagByUrl(string nodeUrl)
        {
            foreach (var memeber in Members)
            {
                if (memeber.Value == nodeUrl)
                {
                    return (true, memeber.Key);
                }
            }
            foreach (var promotable in Promotables)
            {
                if (promotable.Value == nodeUrl)
                {
                    return (true, promotable.Key);
                }
            }
            foreach (var watcher in Watchers)
            {
                if (watcher.Value == nodeUrl)
                {
                    return (true, watcher.Key);
                }
            }
            return (false, (string)null);
        }

        public ClusterTopology()
        {
            
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TopologyId)] = TopologyId,
                [nameof(ApiKey)] = ApiKey,
                [nameof(Members)] = DynamicJsonValue.Convert(Members),
                [nameof(Promotables)] = DynamicJsonValue.Convert(Promotables),
                [nameof(Watchers)] = DynamicJsonValue.Convert(Watchers),
                [nameof(LastNodeId)] = LastNodeId
            };
        }

        public string GetUrlFromTag(string tag)
        {
            string url;
            if (Members.TryGetValue(tag, out url))
            {
                return url;
            }
            if (Promotables.TryGetValue(tag, out url))
            {
                return url;
            }
            if (Watchers.TryGetValue(tag, out url))
            {
                return url;
            }
            return null;
        }

        public static (Dictionary<TKey, TValue> addedValues, Dictionary<TKey, TValue> removedValues) 
            DictionaryDiff<TKey, TValue>(Dictionary<TKey, TValue> oldDic,Dictionary<TKey, TValue> newDic)
        {
            var addedValues = new Dictionary<TKey, TValue>();
            var removedValues = new Dictionary<TKey, TValue>();
            var temp = new Dictionary<TKey,TValue>(newDic);

            foreach (var kvp in oldDic)
            {
                var key = kvp.Key;
                if (temp.TryGetValue(key, out TValue value))
                {
                    if (temp[key] == null || temp[key].Equals(value) == false)
                    {
                        removedValues.Add(key, value);
                    }
                    temp.Remove(key);
                }
                else
                {
                    removedValues.Add(key, value);
                }
                
            }
            foreach (var kvp in temp)
            {
                addedValues.Add(kvp.Key,kvp.Value);
            }
            
            return (addedValues, removedValues);
        }

        public Dictionary<string,string> AllNodes
        {
            get
            {
                var dic = new Dictionary<string,string>();
                foreach (var node in Members)
                {
                    dic[node.Key] = node.Value;
                }
                foreach (var node in Promotables)
                {
                    dic[node.Key] = node.Value;
                }
                foreach (var node in Watchers)
                {
                    dic[node.Key] = node.Value;
                }
                return dic;
            }
        }

        public readonly string LastNodeId;
        public readonly string TopologyId;
        public readonly string ApiKey;
        public readonly Dictionary<string,string> Members;
        public readonly Dictionary<string,string> Promotables;
        public readonly Dictionary<string,string> Watchers;
    }
}