// -----------------------------------------------------------------------
//  <copyright file="SmugglerStreamSourceBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Common
{
    public abstract class SmugglerStreamSourceBase
    {
        protected Task SkipAsync(JsonReader reader, CancellationToken cancellationToken)
        {
            while (reader.Read() && reader.TokenType != JsonToken.EndArray)
            {
                cancellationToken.ThrowIfCancellationRequested();

                RavenJToken.ReadFrom(reader);
            }

            return new CompletedTask();
        }
    }
}