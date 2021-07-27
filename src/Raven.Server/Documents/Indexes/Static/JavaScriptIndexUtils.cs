using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Sparrow.Json;
using Raven.Server.Documents.Patch;


namespace Raven.Server.Documents.Indexes.Static.Utils
{
    public class JavaScriptIndexUtilsBase<TJavaScriptUtils, TEngine>
    {
        public readonly TJavaScriptUtils JavaScriptUtils;
        public readonly TEngine Engine;

        public JavaScriptIndexUtilsBase(TJavaScriptUtils javaScriptUtils)
        {
            JavaScriptUtils = javaScriptUtils;
            Engine = JavaScriptUtils.Engine;
        }

    }
}
