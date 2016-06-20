// -----------------------------------------------------------------------
//  <copyright file="ConfigTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using Raven.Abstractions.Extensions;
using Xunit;
using Xunit.Extensions;

using Raven.Database.FileSystem.Extensions;
using Raven.Json.Linq;


namespace Raven.Tests.FileSystem.Storage
{
    public class ConfigTests : StorageAccessorTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void ConfigExists(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.False(accessor.ConfigExists("config1")));
                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject()));

                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config1")));

                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject()));
                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config1")));
                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config2")));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void SetConfig(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject
                                                                        {
                                                                            { "option1", "value1" }
                                                                        }));

                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject
                                                                        {
                                                                            { "option1", "value2" },
                                                                            { "option2", "value1" }
                                                                        }));

                storage.Batch(accessor =>
                {
                    var config1 = accessor.GetConfig("config1");
                    var config2 = accessor.GetConfig("config2");

                    Assert.NotNull(config1);
                    Assert.Equal(1, config1.Values().Count());
                    Assert.Equal("value1", config1["option1"]);

                    Assert.NotNull(config2);
                    Assert.Equal(2, config2.Values().Count());
                    Assert.Equal("value2", config2["option1"]);
                    Assert.Equal("value1", config2["option2"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void DeleteConfig(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => accessor.DeleteConfig("config1"));

                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject()));

                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config1")));
                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config2")));

                storage.Batch(accessor => accessor.DeleteConfig("config2"));

                storage.Batch(accessor => Assert.True(accessor.ConfigExists("config1")));
                storage.Batch(accessor => Assert.False(accessor.ConfigExists("config2")));

                storage.Batch(accessor => accessor.DeleteConfig("config1"));

                storage.Batch(accessor => Assert.False(accessor.ConfigExists("config1")));
                storage.Batch(accessor => Assert.False(accessor.ConfigExists("config2")));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetConfig(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Throws<FileNotFoundException>(() => accessor.GetConfig("config1")));

                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject 
                                                                        {
                                                                            { "option1", "value1" }
                                                                        }));

                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject 
                                                                        {
                                                                            { "option1", "value2" },
                                                                            { "option2", "value1" }
                                                                        }));

                storage.Batch(accessor =>
                {
                    var config1 = accessor.GetConfig("config1");
                    var config2 = accessor.GetConfig("config2");

                    Assert.NotNull(config1);
                    Assert.Equal(1, config1.Values().Count());
                    Assert.Equal("value1", config1["option1"]);

                    Assert.NotNull(config2);
                    Assert.Equal(2, config2.Values().Count());
                    Assert.Equal("value2", config2["option1"]);
                    Assert.Equal("value1", config2["option2"]);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetConfigNames(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.GetConfigNames(0, 10).ToList()));

                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config3", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config4", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config5", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config6", new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var names = accessor
                        .GetConfigNames(0, 10)
                        .ToList();

                    Assert.Equal(6, names.Count);
                    Assert.Contains("config1", names);
                    Assert.Contains("config2", names);
                    Assert.Contains("config3", names);
                    Assert.Contains("config4", names);
                    Assert.Contains("config5", names);
                    Assert.Contains("config6", names);

                    names = accessor
                        .GetConfigNames(0, 1)
                        .ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Contains("config1", names);

                    names = accessor
                        .GetConfigNames(1, 1)
                        .ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Contains("config2", names);

                    names = accessor
                        .GetConfigNames(2, 2)
                        .ToList();

                    Assert.Equal(2, names.Count);
                    Assert.Contains("config3", names);
                    Assert.Contains("config4", names);

                    names = accessor
                        .GetConfigNames(3, 7)
                        .ToList();

                    Assert.Equal(3, names.Count);
                    Assert.Contains("config4", names);
                    Assert.Contains("config5", names);
                    Assert.Contains("config6", names);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetConfigNamesStartingWithPrefix(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                int total;
                storage.Batch(accessor => Assert.Empty(accessor.GetConfigNamesStartingWithPrefix("config", 0, 10, out total).ToList()));

                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config3", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config4", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config5", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("config6", new RavenJObject()));

                storage.Batch(accessor => accessor.SetConfig("a-config1", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("a-config2", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("a-config3", new RavenJObject()));

                storage.Batch(accessor => accessor.SetConfig("d-config", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("e-config", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("f-config", new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 0, 10, out total)
                        .ToList();

                    Assert.Equal(6, names.Count);
                    Assert.Equal(6, total);
                    Assert.Contains("config1", names);
                    Assert.Contains("config2", names);
                    Assert.Contains("config3", names);
                    Assert.Contains("config4", names);
                    Assert.Contains("config5", names);
                    Assert.Contains("config6", names);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 0, 1, out total)
                        .ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Equal(6, total);
                    Assert.Contains("config1", names);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 1, 1, out total)
                        .ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Equal(6, total);
                    Assert.Contains("config2", names);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 2, 2, out total)
                        .ToList();

                    Assert.Equal(2, names.Count);
                    Assert.Equal(6, total);
                    Assert.Contains("config3", names);
                    Assert.Contains("config4", names);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 3, 7, out total)
                        .ToList();

                    Assert.Equal(3, names.Count);
                    Assert.Equal(6, total);
                    Assert.Contains("config4", names);
                    Assert.Contains("config5", names);
                    Assert.Contains("config6", names);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 7, 2, out total)
                        .ToList();

                    Assert.Equal(0, names.Count);
                    Assert.Equal(6, total);

                    names = accessor
                       .GetConfigNamesStartingWithPrefix("config", 10, 2, out total)
                       .ToList();

                    Assert.Equal(0, names.Count);
                    Assert.Equal(6, total);

                    names = accessor
                      .GetConfigNamesStartingWithPrefix("no-such-key", 0, 2, out total)
                      .ToList();

                    Assert.Equal(0, names.Count);
                    Assert.Equal(0, total);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("config", 10, 5, out total)
                        .ToList();

                    Assert.Equal(0, names.Count);
                    Assert.Equal(6, total);

                    names = accessor
                        .GetConfigNamesStartingWithPrefix("f-config", 12, 2, out total)
                        .ToList();

                    Assert.Equal(0, names.Count);
                    Assert.Equal(1, total);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetConfigNamesStartingWithPrefix2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                int total;

                storage.Batch(accessor => accessor.SetConfig("a-config1", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("b-config2", new RavenJObject()));
                storage.Batch(accessor => accessor.SetConfig("c-config3", new RavenJObject()));

                storage.Batch(accessor =>
                {
                    var names = accessor
                        .GetConfigNamesStartingWithPrefix("a-config", 0, 10, out total)
                        .ToList();

                    Assert.Equal(1, names.Count);

                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetConfigsStartWithPrefix(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                int totalCount;
                storage.Batch(accessor => Assert.Empty(accessor.GetConfigsStartWithPrefix("config", 0, 10, out totalCount).ToList()));

                storage.Batch(accessor => accessor.SetConfig("config1", new RavenJObject { { "option1", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("config2", new RavenJObject { { "option2", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("config3", new RavenJObject { { "option3", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("config4", new RavenJObject { { "option4", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("config5", new RavenJObject { { "option5", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("config6", new RavenJObject { { "option6", "value1" } }));

                storage.Batch(accessor => accessor.SetConfig("a-config1", new RavenJObject { { "option1", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("a-config2", new RavenJObject { { "option2", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("a-config3", new RavenJObject { { "option3", "value1" } }));

                storage.Batch(accessor => accessor.SetConfig("d-config", new RavenJObject { { "option1", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("e-config", new RavenJObject { { "option2", "value1" } }));
                storage.Batch(accessor => accessor.SetConfig("f-config", new RavenJObject { { "option3", "value1" } }));

                storage.Batch(accessor =>
                {
                    var configs = accessor
                        .GetConfigsStartWithPrefix("config", 0, 10, out totalCount)
                        .ToList();

                    Assert.Equal(6, configs.Count);
                    Assert.Equal(6, totalCount);
                    Assert.Equal("value1", configs[0]["option1"]);
                    Assert.Equal("value1", configs[1]["option2"]);
                    Assert.Equal("value1", configs[2]["option3"]);
                    Assert.Equal("value1", configs[3]["option4"]);
                    Assert.Equal("value1", configs[4]["option5"]);
                    Assert.Equal("value1", configs[5]["option6"]);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 0, 1, out totalCount)
                        .ToList();

                    Assert.Equal(1, configs.Count);
                    Assert.Equal(6, totalCount);
                    Assert.Equal("value1", configs[0]["option1"]);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 1, 1, out totalCount)
                        .ToList();

                    Assert.Equal(1, configs.Count);
                    Assert.Equal(6, totalCount);
                    Assert.Equal("value1", configs[0]["option2"]);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 2, 2, out totalCount)
                        .ToList();

                    Assert.Equal(2, configs.Count);
                    Assert.Equal(6, totalCount);
                    Assert.Equal("value1", configs[0]["option3"]);
                    Assert.Equal("value1", configs[1]["option4"]);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 3, 7, out totalCount)
                        .ToList();

                    Assert.Equal(6, totalCount);
                    Assert.Equal(3, configs.Count);
                    Assert.Equal("value1", configs[0]["option4"]);
                    Assert.Equal("value1", configs[1]["option5"]);
                    Assert.Equal("value1", configs[2]["option6"]);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 30, 10, out totalCount)
                        .ToList();

                    Assert.Equal(6, totalCount);
                    Assert.Equal(0, configs.Count);

                    configs = accessor
                        .GetConfigsStartWithPrefix("config", 6, 2, out totalCount)
                        .ToList();

                    Assert.Equal(0, configs.Count);
                    Assert.Equal(6, totalCount);

                    configs = accessor
                        .GetConfigsStartWithPrefix("f-config", 12, 2, out totalCount)
                        .ToList();

                    Assert.Equal(0, configs.Count);
                    Assert.Equal(1, totalCount);
                });
            }
        }
    }
}
