// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3173.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3173 : RavenTestBase
    {

        public class Item
        {
            public string Id { get; set; }
        }

        [Fact]
        public void CannotSaveInvalidHeaders()
        {
            char[] illegalHeaderChar =
            {
                '(', ')', '<', '>', '@'
                , ',', ';', ':', '\\',
                '/', '[', ']', '?', '=',
                '{', '}', (char) 9 /*HT*/, (char) 32 /*SP*/
            };
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                foreach (var illegalChar in illegalHeaderChar)
                {
                    using (var s = store.OpenSession())
                    {
                        var wasExceptionThrown = false;
                        var entity = new Item();
                        s.Store(entity);
                        s.Advanced.GetMetadataFor(entity)["areYouTypeOfillegalChar" + illegalChar] = true;
                        try
                        {
                            s.SaveChanges();
                        }
                        catch (Exception e)
                        {
                            wasExceptionThrown = true;
                        }

                        if (!wasExceptionThrown)
                        {
                            throw new Exception(string.Format("illeagal charecter {0} was accepted", illegalChar));
                        }

                    }
                }
            }
        }
    }
}