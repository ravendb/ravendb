// -----------------------------------------------------------------------
//  <copyright file="CommonSmugglerOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Database.Smuggler.Common
{
    public abstract class CommonSmugglerOptions
    {
        private int batchSize;

        public CommonSmugglerOptions()
        {
            Limit = int.MaxValue;
            BatchSize = 16 * 1024;
        }

        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                if (value < 1)
                    throw new InvalidOperationException("Batch size cannot be zero or a negative number");
                batchSize = value;
            }
        }

        public int Limit { get; set; }
    }
}