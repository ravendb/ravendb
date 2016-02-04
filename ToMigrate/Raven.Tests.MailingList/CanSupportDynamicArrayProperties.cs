using System;
using System.Collections.Generic;
using System.Dynamic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Helpers;
using Xunit;

namespace RavenDB.DynamicArrayProperties.RavenTests
{
    public class CanSupportDynamicArrayProperties : RavenTestBase
    {
        [Fact]
        public void CanSerializeWithJson()
        {
            dynamic product = new CaseInsensitiveDynamicObject();
            product.Title = "RavenDB";
            product.Version = "3.0.3528";
            product.Tags = new[] {"COOL", "NOSQL", "DB"};

            var json = JsonConvert.SerializeObject(product).Replace("\"", "'");
            Assert.Contains("'Tags':['COOL','NOSQL','DB']", json);
        }

        [Fact]
        public void CanSerializeThroughRavenDb()
        {
            dynamic product = new CaseInsensitiveDynamicObject();
            product.Title = "RavenDB";
            product.Version = "3.0.3528";
            product.Tags = new[] { "COOL", "NOSQL", "DB" };

            using (var store = NewDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer += serializer =>
                {
                    serializer.TypeNameHandling = TypeNameHandling.None;
                };
                string id = null;
                using (var session = store.OpenSession())
                {
                    session.Store(product);
                    session.SaveChanges();
                    id = product.Id;
                }

                var doc = store.DatabaseCommands.Get(id);
                var json = doc.ToJson().ToString(Formatting.None).Replace("\"", "'");

                Assert.Contains("'Tags':['COOL','NOSQL','DB']", json);
            }
        }

        public class CaseInsensitiveDynamicObject : DynamicObject
        {
            public IDictionary<string, object> Dictionary { get; private set; }

            public CaseInsensitiveDynamicObject()
            {
                Dictionary = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            }

            public override bool TryGetMember(GetMemberBinder binder, out object result)
            {
                return Dictionary.TryGetValue(binder.Name, out result);
            }

            public override bool TrySetMember(SetMemberBinder binder, object value)
            {
                Dictionary[binder.Name] = value;
                return true;
            }

            public override bool TryDeleteMember(DeleteMemberBinder binder)
            {
                Dictionary.Remove(binder.Name);
                return true;
            }

            public override IEnumerable<string> GetDynamicMemberNames()
            {
                return Dictionary.Keys;
            }
        } 
    }
}
