﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IGraphQueryStep
    {
        ValueTask Initialize(long? cutoffEtag, Stopwatch queryDuration, TimeSpan? queryWaitDuration);

        HashSet<string> GetAllAliases();

        string GetOutputAlias();

        bool GetNext(out GraphQueryRunner.Match match);

        List<GraphQueryRunner.Match> GetById(string id);

        void Analyze(GraphQueryRunner.Match match,
            GraphQueryRunner.GraphDebugInfo graphDebugInfo);

        bool IsEmpty();

        IGraphQueryStep Clone();

        ISingleGraphStep GetSingleGraphStepExecution();
    }

    public interface ISingleGraphStep
    {
        ValueTask Initialize(long? cutoffEtag, Stopwatch queryDuration, TimeSpan? queryWaitDuration);

        bool GetAndClearResults(List<GraphQueryRunner.Match> matches);

        void Run(GraphQueryRunner.Match src, string alias);

        void AddAliases(HashSet<string> aliases);

        void SetPrev(IGraphQueryStep prev);
    }
}
