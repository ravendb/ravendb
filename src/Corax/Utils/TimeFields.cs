using System.Collections.Generic;
using System.IO;
using Voron;
using Voron.Impl;

namespace Corax.Utils;

/// <summary>
/// Store fields names inside multitree. This allows us to be sure we can perform query over ticks (instead strings).
/// </summary>
public static class TimeFields
{
    public static HashSet<string> ReadTimeFieldsNames(Transaction tx)
    {
        var fieldsNames = new HashSet<string>();

        var timeFieldsTree = tx.ReadTree(Constants.IndexTimeFieldsSlice);
        if (timeFieldsTree != null)
        {
            using var iterator = timeFieldsTree.MultiRead(Constants.IndexTimeFieldsSlice);
            if (iterator.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    fieldsNames.Add(iterator.CurrentKey.ToString());
                } while (iterator.MoveNext());
            }
        }

        return fieldsNames;
    }

    public static void WriteTimeFieldsNames(Transaction tx, HashSet<string> fieldsNames)
    {
        if (tx.IsWriteTransaction == false)
            throw new InvalidDataException("Tried to write fields names but got non write transaction.");

        if (fieldsNames == null || fieldsNames.Count == 0)
            return;

        using var indexTimeFieldsTree = tx.CreateTree(Constants.IndexTimeFieldsSlice);
        foreach (var field in fieldsNames)
            indexTimeFieldsTree.MultiAdd(Constants.IndexTimeFieldsSlice, field);
    }
}
