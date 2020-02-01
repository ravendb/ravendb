// -----------------------------------------------------------------------
//  <copyright file="ThreadInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace Raven.Debug
{
    internal class ThreadInfo
    {
        public uint OSThreadId;

        public int ManagedThreadId;

        public bool IsNative;

        [JsonConverter(typeof(StringEnumConverter))]
        public ThreadType ThreadType;

        public List<string> StackTrace = new List<string>();

        public List<string> StackObjects;
    }

    public enum ThreadType
    {
        Other,
        GC,
        Finalizer,
        Native
    }
}
