// -----------------------------------------------------------------------
//  <copyright file="ReduceType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.NewClient.Abstractions.Data
{
    public enum ReduceType
    {
        None = 0,
        SingleStep = 1,
        MultiStep = 2,
    }
}
