//-----------------------------------------------------------------------
// <copyright file="IGeneralStorageActions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Database.Storage
{
    public interface IGeneralStorageActions
    {
        long GetNextIdentityValue(string name, int val = 1);
        void SetIdentityValue(string name, long value);
        IEnumerable<KeyValuePair<string, long>> GetIdentities(int start, int take, out long totalCount);

        void PulseTransaction();
        bool MaybePulseTransaction(int addToPulseCount = 1, Action beforePulseTransaction = null);
    }
}
