//-----------------------------------------------------------------------
// <copyright file="RobustEnumerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Database.Linq;
using Raven.Database.Storage;

namespace Raven.Database.Indexing
{
    public class RobustEnumerator
    {
        public Action BeforeMoveNext = delegate { };
        public Action CancelMoveNext = delegate { };
        public Action<Exception, object> OnError = delegate { };


        public IEnumerable<object> RobustEnumeration(IEnumerable<object> input, IndexingFunc func)
        {
            var wrapped = new StatefulEnumerableWrapper<dynamic>(input.GetEnumerator());
            IEnumerator<object> en = func(wrapped).GetEnumerator();
            do
            {
                var moveSuccessful = MoveNext(en, wrapped);
                if (moveSuccessful == false)
                    yield break;
                if (moveSuccessful == true)
                    yield return en.Current;
                else
                    en = func(wrapped).GetEnumerator();
            } while (true);
        }

        private bool? MoveNext(IEnumerator en, StatefulEnumerableWrapper<object> innerEnumerator)
        {
            try
            {
                BeforeMoveNext();
                var moveNext = en.MoveNext();
                if (moveNext == false)
                {
                    CancelMoveNext();
                }
                return moveNext;
            }
            catch (Exception e)
            {
                OnError(e, innerEnumerator.Current);
            }
            return null;
        }
    }
}