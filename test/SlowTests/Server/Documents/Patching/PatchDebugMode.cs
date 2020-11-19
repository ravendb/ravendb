using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchDebugMode : RavenTestBase
    {
        public PatchDebugMode(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DebugResultsWhenLoadingData()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today;
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    session.CountersFor("cars/1").Increment("Counter1", 100);
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline.AddMinutes(1), new[] { 11d, 22d }, "tag1");
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                        Script = @"
                                    load('cars/1');
                                    load('cars/2');

                                    counter('cars/1', 'Counter1');
                                    counter('cars/1', 'Counter2');

                                    timeseries(this, 'TimeSeries1').get();
                                    timeseries(this, 'TimeSeries2').get();
                                 "
                        }, patchIfMissing: null);

                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");

                    // Documents Load Check
                    Assert.True(actions.TryGet("LoadDocument", out BlittableJsonReaderArray loadDocument), "<LoadDocument> section not found");

                    (loadDocument[0] as BlittableJsonReaderObject).TryGet("Id", out string id); 
                    Assert.Equal("cars/1", id);
                    (loadDocument[0] as BlittableJsonReaderObject).TryGet("Exists", out bool exists);
                    Assert.Equal(true, exists);
                    
                    (loadDocument[1] as BlittableJsonReaderObject).TryGet("Id", out id); 
                    Assert.Equal("cars/2", id);
                    (loadDocument[1] as BlittableJsonReaderObject).TryGet("Exists", out exists);
                    Assert.Equal(false, exists);
                    
                    // Counters Load Check
                    Assert.True(actions.TryGet("GetCounter", out BlittableJsonReaderArray getCounter), "<GetCounter> section not found");

                    (getCounter[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("Counter1", name);
                    (getCounter[0] as BlittableJsonReaderObject).TryGet("Value", out string value);
                    Assert.Equal("100", value);
                    (getCounter[0] as BlittableJsonReaderObject).TryGet("Exists", out exists);
                    Assert.Equal(true, exists);
                    
                    (getCounter[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("Counter2", name);
                    (getCounter[1] as BlittableJsonReaderObject).TryGet("Exists", out exists);
                    Assert.Equal(false, exists);
                    
                    // Time Series Load Check
                    Assert.True(actions.TryGet("GetTimeSeries", out BlittableJsonReaderArray getTimeSeries), "<GetTimeSeries> section not found");

                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("TimeSeries1", name);
                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Timestamp", out DateTime timestamp);
                     Assert.Equal(baseline.AddMinutes(1), timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Tag", out string tag);
                    Assert.Equal("tag1", tag);
                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Type", out string type);
                    Assert.Equal("Raw", type); 
                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Exists", out exists);
                    Assert.Equal(true, exists);

                    (getTimeSeries[0] as BlittableJsonReaderObject).TryGet("Values", out BlittableJsonReaderArray values);
                    Assert.Equal("11.0", values[0].ToString()); 
                    Assert.Equal("22.0", values[1].ToString()); 

                    (getTimeSeries[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("TimeSeries2", name);
                    (getTimeSeries[1] as BlittableJsonReaderObject).TryGet("Exists", out exists);
                    Assert.Equal(false, exists);
                }
            }
        }

        [Fact]
        public void DebugResultsWhenModifyingDocuments()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Car {Model = "Golf"}, "cars/1");
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                         Script = @"
                                     put('cars/2', { 'Model' : 'Toyota' });
                                  "
                    }, patchIfMissing: null);
                    
                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");

                    // Document Put Check
                    Assert.True(actions.TryGet("PutDocument", out BlittableJsonReaderArray putDocument), "<PutDocument> section not found");

                    (putDocument[0] as BlittableJsonReaderObject).TryGet("Id", out string id); 
                    Assert.Equal("cars/2", id);
                    (putDocument[0] as BlittableJsonReaderObject).TryGet("Data", out BlittableJsonReaderObject data);
                    data.TryGet("Model", out string model);
                    Assert.Equal("Toyota", model);
                }
            }
        }
        
        [Fact]
        public void DebugResultsWhenModifyingCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    session.CountersFor("cars/1").Increment("Counter1", 100);
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                        Script = @"
                                    incrementCounter('cars/1', 'Counter1', 50);
                                    incrementCounter('cars/1', 'Counter2');
                                 "
                    }, patchIfMissing: null);
                    
                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Counters Modifications Check
                    Assert.True(actions.TryGet("IncrementCounter", out BlittableJsonReaderArray incrementCounter), "<IncrementCounter> section not found");

                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("Counter1", name);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("OldValue", out int? oldValue);
                    Assert.Equal(100, oldValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("AddedValue", out int addedValue);
                    Assert.Equal(50, addedValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("NewValue", out int newValue);
                    Assert.Equal(150, newValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("Created", out bool created);
                    Assert.Equal(false, created);
                    
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("Counter2", name);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("OldValue", out oldValue);
                    Assert.Equal(null, oldValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("AddedValue", out addedValue);
                    Assert.Equal(1, addedValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("NewValue", out newValue);
                    Assert.Equal(1, newValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(true, created);
                }
            }
        }
        
        [Fact]
        public void DebugResultsWhenModifyingTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline, new[] { 11d, 22d }, "tag1");
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                        Script = @"
                                    timeseries('cars/1', 'TimeSeries1').append(args.timestamp0, [33, 44], 'tag2');
                                    timeseries('cars/1', 'TimeSeries1').append(args.timestamp1, [55], 'tag3');
                                    timeseries('cars/1', 'TimeSeries2').append(args.timestamp2, [77], 'tag4');
                                 ",
                        Values =
                        {
                            { "timestamp0", baseline },
                            { "timestamp1", baseline.AddMinutes(1) },
                            { "timestamp2", baseline.AddMinutes(2) }
                        }
                    }, patchIfMissing: null);
                    
                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Time Series Modifications Check
                    Assert.True(actions.TryGet("AppendTimeSeries", out BlittableJsonReaderArray appendTimeSeries), "<AppendTimeSeries> section not found");

                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("TimeSeries1", name);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Timestamp", out DateTime timestamp);
                    Assert.Equal(baseline, timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Tag", out string tag);
                    Assert.Equal("tag2", tag);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Values", out BlittableJsonReaderArray values);
                    Assert.Equal("33.0", values[0].ToString());
                    Assert.Equal("44.0", values[1].ToString());
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Created", out bool created);
                    Assert.Equal(false, created);
                    
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Name", out  name); 
                    Assert.Equal("TimeSeries1", name);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Timestamp", out timestamp);
                    Assert.Equal(baseline.AddMinutes(1), timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Tag", out tag);
                    Assert.Equal("tag3", tag);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Values", out values);
                    Assert.Equal("55.0", values[0].ToString());
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(false, created);
                    
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Name", out  name); 
                    Assert.Equal("TimeSeries2", name);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Timestamp", out timestamp);
                    Assert.Equal(baseline.AddMinutes(2), timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Tag", out tag);
                    Assert.Equal("tag4", tag);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Values", out values);
                    Assert.Equal("77.0", values[0].ToString());
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(true, created);
                }
            }
        }
        
        [Fact]
        public void DebugResultsWhenDeletingDocumentsAndCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    session.CountersFor("cars/1").Increment("Counter1", 100);
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                        Script = @"
                                    del('cars/2');
                                    deleteCounter('cars/1', 'Counter1');
                                 "
                    }, patchIfMissing: null);

                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Document Delete Check
                    Assert.True(actions.TryGet("DeleteDocument", out BlittableJsonReaderArray deleteDocument), "<DeleteDocument> section not found");
                    Assert.Equal("cars/2", deleteDocument[0].ToString());

                    // Counters Delete Check
                    Assert.True(actions.TryGet("DeleteCounter", out BlittableJsonReaderArray deleteCounter), "<DeleteCounter> section not found");
                    Assert.Equal("Counter1", deleteCounter[0].ToString());
                }
            }
        }
        
        [Fact]
        public void DebugResultsWhenDeletingTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline, new[] { 11d, 11d }, "tag1");
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline.AddMinutes(1), new[] { 22d, 22d }, "tag2");
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline.AddMinutes(2), new[] { 33d, 33d }, "tag3");
                    
                    session.TimeSeriesFor("cars/1", "TimeSeries2").Append(baseline, new[] { 44d, 44d }, "tag4");
                    
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                        Script = @"
                                    timeseries('cars/1', 'TimeSeries1').delete(args.from, args.to);
                                    timeseries('cars/1', 'TimeSeries2').delete(args.min, args.max);
                                 ", 
                        Values =
                        {
                            { "from", baseline },
                            { "to", baseline.AddMinutes(1) },
                            { "min", DateTime.MinValue },
                            { "max", DateTime.MaxValue }
                        }
                    }, patchIfMissing: null);
                    
                    
                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, true, true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Time Series Delete Check
                    Assert.True(actions.TryGet("DeleteTimeSeries", out BlittableJsonReaderArray deleteTimeSeries), "<DeleteTimeSeries> section not found");
                    
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("TimeSeries1", name);
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("From", out DateTime from);
                    Assert.Equal(baseline, from, RavenTestHelper.DateTimeComparer.Instance);
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("To", out DateTime to);
                    Assert.Equal(baseline.AddMinutes(1), to, RavenTestHelper.DateTimeComparer.Instance);
                    
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("TimeSeries2", name);
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("From", out from);
                    Assert.Equal(DateTime.MinValue, from, RavenTestHelper.DateTimeComparer.Instance);
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("To", out to);
                    Assert.Equal(DateTime.MaxValue, to, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }

        [Fact]
        public void DebugResultsWhenModifyingData()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.EnsureUtc();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    session.CountersFor("cars/1").Increment("Counter1", 100);
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline, new[] { 11d, 22d }, "tag1");
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                         Script = @"
                                      put('cars/2', { 'Model' : 'Toyota' });

                                      incrementCounter('cars/1', 'Counter1', 50);
                                      incrementCounter('cars/1', 'Counter2');

                                      timeseries('cars/1', 'TimeSeries1').append(args.timestamp0, [33, 44], 'tag2');
                                      timeseries('cars/1', 'TimeSeries1').append(args.timestamp1, [55], 'tag3');
                                      timeseries('cars/1', 'TimeSeries2').append(args.timestamp2, [77], 'tag4');
                                   ",
                          Values =
                          {
                              { "timestamp0", baseline },
                              { "timestamp1", baseline.AddMinutes(1) },
                              { "timestamp2", baseline.AddMinutes(2) }
                          }
                    }, patchIfMissing: null);

                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, returnDebugInformation: true, test: true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Document Put Check
                    Assert.True(actions.TryGet("PutDocument", out BlittableJsonReaderArray putDocument), "<PutDocument> section not found");

                    (putDocument[0] as BlittableJsonReaderObject).TryGet("Id", out string id); 
                    Assert.Equal("cars/2", id);
                    (putDocument[0] as BlittableJsonReaderObject).TryGet("Data", out BlittableJsonReaderObject data);
                    data.TryGet("Model", out string model);
                    Assert.Equal("Toyota", model);
                    
                    // Counters Modifications Check
                    Assert.True(actions.TryGet("IncrementCounter", out BlittableJsonReaderArray incrementCounter), "<IncrementCounter> section not found");

                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("Counter1", name);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("OldValue", out int? oldValue);
                    Assert.Equal(100, oldValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("AddedValue", out int addedValue);
                    Assert.Equal(50, addedValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("NewValue", out int newValue);
                    Assert.Equal(150, newValue);
                    (incrementCounter[0] as BlittableJsonReaderObject).TryGet("Created", out bool created);
                    Assert.Equal(false, created);
                    
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("Counter2", name);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("OldValue", out oldValue);
                    Assert.Equal(null, oldValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("AddedValue", out addedValue);
                    Assert.Equal(1, addedValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("NewValue", out newValue);
                    Assert.Equal(1, newValue);
                    (incrementCounter[1] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(true, created);
                    
                    // Time Series Modifications Check
                    Assert.True(actions.TryGet("AppendTimeSeries", out BlittableJsonReaderArray appendTimeSeries), "<AppendTimeSeries> section not found");

                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("TimeSeries1", name);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Timestamp", out DateTime timestamp);
                    Assert.Equal(baseline, timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Tag", out string tag);
                    Assert.Equal("tag2", tag);
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Values", out BlittableJsonReaderArray values);
                    Assert.Equal("33.0", values[0].ToString());
                    Assert.Equal("44.0", values[1].ToString());
                    (appendTimeSeries[0] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(false, created);
                    
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Name", out  name); 
                    Assert.Equal("TimeSeries1", name);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Timestamp", out timestamp);
                    Assert.Equal(baseline.AddMinutes(1), timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Tag", out tag);
                    Assert.Equal("tag3", tag);
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Values", out values);
                    Assert.Equal("55.0", values[0].ToString());
                    (appendTimeSeries[1] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(false, created);
                    
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Name", out  name); 
                    Assert.Equal("TimeSeries2", name);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Timestamp", out timestamp);
                    Assert.Equal(baseline.AddMinutes(2), timestamp, RavenTestHelper.DateTimeComparer.Instance);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Tag", out tag);
                    Assert.Equal("tag4", tag);
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Values", out values);
                    Assert.Equal("77.0", values[0].ToString());
                    (appendTimeSeries[2] as BlittableJsonReaderObject).TryGet("Created", out created);
                    Assert.Equal(true, created);
                }
            }
        }
        
        [Fact]
        public void DebugResultsWhenDeletingData()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.ToUniversalTime();
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Car { Model = "Golf" }, "cars/1");
                    
                    session.CountersFor("cars/1").Increment("Counter1", 100);
                    
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline, new[] { 11d, 11d }, "tag1");
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline.AddMinutes(1), new[] { 22d, 22d }, "tag2");
                    session.TimeSeriesFor("cars/1", "TimeSeries1").Append(baseline.AddMinutes(2), new[] { 33d, 33d }, "tag3");
                    
                    session.TimeSeriesFor("cars/1", "TimeSeries2").Append(baseline, new[] { 44d, 44d }, "tag4");
                    
                    session.SaveChanges();
                }
                
                var reqEx = store.GetRequestExecutor();
                using (reqEx.ContextPool.AllocateOperationContext(out var context))
                {
                    PatchOperation operation = new PatchOperation(id: "cars/1", changeVector: null, patch: new PatchRequest {
                         Script = @"
                                    del('cars/2');

                                    deleteCounter('cars/1', 'Counter1');

                                    timeseries('cars/1', 'TimeSeries1').delete(args.from, args.to);
                                    timeseries('cars/1', 'TimeSeries2').delete(args.min, args.max);
                                   ",
                          Values =
                          {
                              { "from", baseline },
                              { "to", baseline.AddMinutes(1) },
                              { "min", DateTime.MinValue },
                              { "max", DateTime.MaxValue }
                          }
                    }, patchIfMissing: null);
                  
                    var cmd = operation.GetCommand(store, store.Conventions, context, reqEx.Cache, returnDebugInformation: true, test: true);
                    store.Commands().Execute(cmd);
                    
                    Assert.True(cmd.Result.Debug.TryGet("Actions", out BlittableJsonReaderObject actions), "<Actions> section not found");
                    
                    // Document Delete Check
                    Assert.True(actions.TryGet("DeleteDocument", out BlittableJsonReaderArray deleteDocument), "<DeleteDocument> section not found");
                    Assert.Equal("cars/2", deleteDocument[0].ToString());

                    // Counters Delete Check
                    Assert.True(actions.TryGet("DeleteCounter", out BlittableJsonReaderArray deleteCounter), "<DeleteCounter> section not found");
                    Assert.Equal("Counter1", deleteCounter[0].ToString());
                    
                    // Time Series Delete Check
                    Assert.True(actions.TryGet("DeleteTimeSeries", out BlittableJsonReaderArray deleteTimeSeries), "<DeleteTimeSeries> section not found");
                    
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("Name", out string name); 
                    Assert.Equal("TimeSeries1", name);
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("From", out DateTime from);
                    Assert.Equal(baseline, from, RavenTestHelper.DateTimeComparer.Instance);
                    (deleteTimeSeries[0] as BlittableJsonReaderObject).TryGet("To", out DateTime to);
                    Assert.Equal(baseline.AddMinutes(1), to, RavenTestHelper.DateTimeComparer.Instance);
                    
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("Name", out name); 
                    Assert.Equal("TimeSeries2", name);
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("From", out from);
                    Assert.Equal(DateTime.MinValue, from, RavenTestHelper.DateTimeComparer.Instance);
                    (deleteTimeSeries[1] as BlittableJsonReaderObject).TryGet("To", out to);
                    Assert.Equal(DateTime.MaxValue, to, RavenTestHelper.DateTimeComparer.Instance);
                }
            }
        }
        
        private class Car
        {
            public string Model { get; set; }
        }
    }
}
