// -----------------------------------------------------------------------
//  <copyright file="AaronSt.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;
using System.Net;
using System.IO;
using Raven.Abstractions.Data;

namespace Raven.Tests.MailingList
{
    public class ValerioBorioni : RavenTest
    {
        [Fact]
        public void RavenJValue_recognize_NAN_Float_isEqual_to_NAN_String()
        {
            using (var store = NewDocumentStore())
            {
                store.Configuration.RunInMemory = true;
                store.Initialize();


                using (var session = store.OpenSession())
                {
                    session.Store(new MyEntity());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var all = session.Query<MyEntity>().Customize(r => r.WaitForNonStaleResults()).ToList();
                    var changes = session.Advanced.WhatChanged();
                    Assert.Empty(changes);
                }
            };

        }

        public class MyEntity
        {
            public double Value { get; set; }

            public MyEntity()
            {
                Value = double.NaN;
            }
        }


        [Fact]
        public void Import_documents_by_csv_should_preserve_documentId_if_id_header_is_present()
        {
            var databaseName = "TestCsvDatabase";
            var entityName = typeof(CsvEntity).Name;
            var documentId = "MyCustomId123abc";

            using (var store = NewRemoteDocumentStore(false, null, databaseName))
            {
                var url = string.Format(@"http://localhost:8079/databases/{0}/studio-tasks/loadCsvFile", databaseName);
                var tempFile = Path.GetTempFileName();

                File.AppendAllLines(tempFile, new[]
                {
                    "id,Property_A,Value,@Ignored_Property," + Constants.RavenEntityName,
                    documentId +",a,123,doNotCare," + entityName
                });

                using (var wc = new WebClient())
                    wc.UploadFile(url, tempFile);

                using (var session = store.OpenSession(databaseName))
                {
                    var entity = session.Load<CsvEntity>(documentId);
                    Assert.NotNull(entity);

                    var metadata = session.Advanced.GetMetadataFor(entity);
                    var ravenEntityName = metadata.Value<string>(Constants.RavenEntityName);
                    Assert.Equal(entityName, ravenEntityName);
                }

            }
        }

        public class CsvEntity
        {
            public string Id { get; set; }
            public double Value { get; set; }

            public CsvEntity()
            {
                Value = double.NaN;
            }
        }

    }
}
