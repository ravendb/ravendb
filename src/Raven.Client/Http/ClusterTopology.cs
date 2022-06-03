using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Http
{
    public class ClusterTopology
    {
        public ClusterTopology(string topologyId, Dictionary<string, string> members, Dictionary<string, string> promotables, Dictionary<string, string> watchers, string lastNodeId, long index)
        {
            TopologyId = topologyId;
            Members = members;
            Promotables = promotables;
            Watchers = watchers;
            LastNodeId = lastNodeId;
            Etag = index;
        }

        internal void ReplaceCurrentNodeUrlWithClientRequestedUrl(string currentNodeTag, string clientRequestedUrl)
        {
            foreach (var member in Members)
            {
                if (member.Key == currentNodeTag)
                {
                    Members[member.Key] = clientRequestedUrl;
                    return;
                }
            }
            foreach (var promotable in Promotables)
            {
                if (promotable.Key == currentNodeTag)
                {
                    Promotables[promotable.Key] = clientRequestedUrl;
                    return;
                }
            }
            foreach (var watcher in Watchers)
            {
                if (watcher.Key == currentNodeTag)
                {
                    Watchers[watcher.Key] = clientRequestedUrl;
                    return;
                }
            }
            
        }

        public bool Contains(string node)
        {
            return Members.ContainsKey(node) || Promotables.ContainsKey(node) || Watchers.ContainsKey(node);
        }

        //Try to avoid using this since it is expensive
        public (bool HasUrl, string NodeTag) TryGetNodeTagByUrl(string nodeUrl)
        {
            foreach (var member in Members)
            {
                if (member.Value == nodeUrl)
                {
                    return (true, member.Key);
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

        public DynamicJsonValue ToSortedJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TopologyId)] = TopologyId,
                [nameof(AllNodes)] = DynamicJsonValue.Convert(new SortedDictionary<string, string>(AllNodes)),
                [nameof(Members)] = DynamicJsonValue.Convert(new SortedDictionary<string, string>(Members)),
                [nameof(Promotables)] = DynamicJsonValue.Convert(new SortedDictionary<string, string>(Promotables)),
                [nameof(Watchers)] = DynamicJsonValue.Convert(new SortedDictionary<string, string>(Watchers)),
                [nameof(LastNodeId)] = LastNodeId,
                [nameof(Etag)] = Etag
            };
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TopologyId)] = TopologyId,
                [nameof(Members)] = DynamicJsonValue.Convert(Members),
                [nameof(Promotables)] = DynamicJsonValue.Convert(Promotables),
                [nameof(Watchers)] = DynamicJsonValue.Convert(Watchers),
                [nameof(LastNodeId)] = LastNodeId,
                [nameof(Etag)] = Etag
            };
        }

        public string GetUrlFromTag(string tag)
        {
            if (tag == null)
                return null;

            if (Members.TryGetValue(tag, out string url) ||
                Promotables.TryGetValue(tag, out url) ||
                Watchers.TryGetValue(tag, out url))
                return url;

            return null;
        }

        public static (Dictionary<TKey, TValue> AddedValues, Dictionary<TKey, TValue> RemovedValues) DictionaryDiff<TKey, TValue>(
            Dictionary<TKey, TValue> oldDic, Dictionary<TKey, TValue> newDic)
        {
            var addedValues = new Dictionary<TKey, TValue>();
            var removedValues = new Dictionary<TKey, TValue>();
            var temp = new Dictionary<TKey, TValue>(newDic);

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
                addedValues.Add(kvp.Key, kvp.Value);
            }
            return (addedValues, removedValues);
        }

        public Dictionary<string, string> AllNodes
        {
            get
            {
                var dic = new Dictionary<string, string>();
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

        public string LastNodeId { get; protected set; }
        public string TopologyId { get; protected set; }
        public long Etag { get; protected set; }

        public Dictionary<string, string> Members { get; protected set; }
        public Dictionary<string, string> Promotables { get; protected set; }
        public Dictionary<string, string> Watchers { get; protected set; }
    }

    public class NodeStatus : IDynamicJson
    {
        public string Name { get; set; }
        public bool Connected { get; set; }
        public string ErrorDetails { get; set; }
        public DateTime LastSent { get; set; }
        public DateTime LastReply { get; set; }
        public string LastSentMessage { get; set; }
        public long LastMatchingIndex { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Connected)] = Connected,
                [nameof(ErrorDetails)] = ErrorDetails,
                [nameof(LastSent)] = LastSent,
                [nameof(LastReply)] = LastReply,
                [nameof(LastSentMessage)] = LastSentMessage,
                [nameof(LastMatchingIndex)] = LastMatchingIndex
            };
        }

        public override string ToString()
        {
            return $"{nameof(Name)}:{Name}, " +
                   $"{nameof(Connected)}:{Connected}, " +
                   $"{nameof(ErrorDetails)}:{ErrorDetails}, " +
                   $"{nameof(LastSent)}:{LastSent}, " +
                   $"{nameof(LastReply)}:{LastReply}, " +
                   $"{nameof(LastSentMessage)}:{LastSentMessage}";
        }
    }
}
