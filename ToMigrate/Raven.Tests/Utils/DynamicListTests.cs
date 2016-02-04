// -----------------------------------------------------------------------
//  <copyright file="DynamicListTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Linq;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Utils
{
    public class DynamicListTests : RavenTest
    {
        [Fact]
        public void needs_to_have_all_public_generic_extension_methods_of_enumerable_class()
        {
            var enumerableMethods = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static).Where(x => x.IsGenericMethod && x.IsDefined(typeof(ExtensionAttribute), false));
            var dynamicListMethods = typeof(DynamicList).GetMethods(BindingFlags.Public | BindingFlags.Instance);

            var exceptFor = new[]
            {
                "Count", // DynamicList has Count property, so it cannot have a method with the same name
                "ThenBy", // applies to IOrderedEnumerable
                "ThenByDescending", // applies to IOrderedEnumerable
            };

            var enumerableMethodsNames = enumerableMethods.Select(x => x.Name).Except(exceptFor).Distinct().ToList();
            var dynamicListMethodsNames = dynamicListMethods.Select(x => x.Name).Distinct().ToList();

            foreach (var enumerableMethod in enumerableMethodsNames)
            {
                Assert.Contains(enumerableMethod, dynamicListMethodsNames);
            }
        }

        [Fact]
        public void sum_min_max_avg_with_integers()
        {
            var sut = CreateDynamicList(new[]
            {
                1, 2, 3
            });

            Assert.Equal(6, sut.Sum(x => x));
            Assert.Equal(1, sut.Min(x => x));
            Assert.Equal(3, sut.Max(x => x));
            Assert.Equal(2, sut.Average(x => x));
        }

        [Fact]
        public void sum_min_max_avg_with_nullable_integers()
        {
            var sut = CreateDynamicList(new int?[]
            {
                null, 1, 2, 3
            });

            Assert.Equal(6, sut.Sum(x => (int?)x));
            Assert.Equal(1, sut.Min(x => (int?)x));
            Assert.Equal(3, sut.Max(x => (int?)x));
            Assert.Equal(2, sut.Average(x => (int?)x));
        }

        [Fact]
        public void sum_min_max_avg_with_longs()
        {
            var sut = CreateDynamicList(new[]
            {
                1L, 2L, 3L
            });

            Assert.Equal(6L, sut.Sum(x => (long)x));
            Assert.Equal(1L, sut.Min(x => (long)x));
            Assert.Equal(3L, sut.Max(x => (long)x));
            Assert.Equal(2L, sut.Average(x => (long)x));
        }

        [Fact]
        public void sum_min_max_avg_with_nulable_longs()
        {
            var sut = CreateDynamicList(new long?[]
            {
                null, 1L, 2L, 3L
            });

            Assert.Equal(6L, sut.Sum(x => (long?)x));
            Assert.Equal(1L, sut.Min(x => (long?)x));
            Assert.Equal(3L, sut.Max(x => (long?)x));
            Assert.Equal(2L, sut.Average(x => (long?)x));
        }

        [Fact]
        public void sum_min_max_avg_with_floats()
        {
            var sut = CreateDynamicList(new[]
            {
                1.1f, 2.2f, 3.3f
            });

            Assert.Equal(6.6f, sut.Sum(x => (float)x));
            Assert.Equal(1.1f, sut.Min(x => (float)x));
            Assert.Equal(3.3f, sut.Max(x => (float)x));
            Assert.Equal(2.2f, sut.Average(x => (float)x));
        }

        [Fact]
        public void sum_min_max_avg_with_nullable_floats()
        {
            var sut = CreateDynamicList(new float?[]
            {
                null, 1.1f, 2.2f, 3.3f
            });

            Assert.Equal(6.6f, sut.Sum(x => (float?)x));
            Assert.Equal(1.1f, sut.Min(x => (float?)x));
            Assert.Equal(3.3f, sut.Max(x => (float?)x));
            Assert.Equal(2.2f, sut.Average(x => (float?)x));
        }

        [Fact]
        public void sum_min_max_avg_with_doubles()
        {
            var sut = CreateDynamicList(new[]
            {
                1.1, 2.2, 3.3
            });

            Assert.Equal(6.6, sut.Sum(x => (double)x));
            Assert.Equal(1.1, sut.Min(x => (double)x));
            Assert.Equal(3.3, sut.Max(x => (double)x));
            Assert.Equal(2.2, sut.Average(x => (double)x), 2);
        }

        [Fact]
        public void sum_min_max_avg_with_nullable_doubles()
        {
            var sut = CreateDynamicList(new double?[]
            {
                1.1, null, 2.2, 3.3
            });

            Assert.Equal(6.6, sut.Sum(x => (double?)x));
            Assert.Equal(1.1, sut.Min(x => (double?)x));
            Assert.Equal(3.3, sut.Max(x => (double?)x));
            Assert.Equal(2.2, sut.Average(x => (double?)x).Value, 2);
        }

        [Fact]
        public void min_and_max_return_object_when_no_selector_given()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.Min();

            Assert.IsType<Fixture>(result);
            Assert.Same(fixtures.Min(), result);

            result = sut.Max();
            Assert.IsType<Fixture>(result);
            Assert.Same(fixtures.Max(), result);
        }

        [Fact]
        public void min_and_max_can_transform_value_and_return_results_of_tranformation_type()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.Min(x => (x.Value + 3) / 2), sut.Min(x => (x.Value + 3) / 2));
            Assert.Equal(fixtures.Max(x => (x.Value + 3) / 2), sut.Max(x => (x.Value + 3) / 2));
        }

        [Fact]
        public void min_and_max_return_dynamic_nullable_for_null_values()
        {
            var sut = CreateDynamicList(new int?[] { null, null });

            Assert.IsType<DynamicNullObject>(sut.Min());
            Assert.IsType<DynamicNullObject>(sut.Max());

            Assert.IsType<DynamicNullObject>(sut.Min(x => (int?)x));
            Assert.IsType<DynamicNullObject>(sut.Max(x => (int?)x));
        }

        [Fact]
        public void average_on_objects()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.Average(x => x.Value), sut.Average(x => x.Value));
        }

        [Fact]
        public void except_on_value_types()
        {
            var items = new List<int>
            {
                1, 2, 3, 4, 5
            };
            var sut = CreateDynamicList(items);

            var result = sut.Except(new dynamic[]{1, 4}).ToList();

            Assert.Equal(3, result.Count());
            Assert.DoesNotContain(1, result);
            Assert.DoesNotContain(4, result);
        }

        [Fact]
        public void except_on_objects()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.Except(new dynamic[] { fixtures[0] }).ToList();

            Assert.Equal(2, result.Count());
            Assert.DoesNotContain(fixtures[0], result);
            Assert.Contains(fixtures[1], result);
            Assert.Contains(fixtures[2], result);
        }

        [Fact]
        public void reverse_returns_inverted_items()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Equal(sut.Reverse(), (fixtures as IEnumerable<Fixture>).Reverse());
        }

        [Fact]
        public void sequence_equal()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.True(sut.SequenceEqual(fixtures));
            Assert.False(sut.SequenceEqual(new []{fixtures[2], fixtures[0], fixtures[1]}));

            Assert.Equal(sut.SequenceEqual(sut), fixtures.SequenceEqual(fixtures));
            Assert.Equal(sut.SequenceEqual(new[] { fixtures[2], fixtures[0], fixtures[1] }), fixtures.SequenceEqual(new[] { fixtures[2], fixtures[0], fixtures[1] }));
        }

        [Fact]
        public void as_enumerable_returns_the_same_instance()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Same(sut.AsEnumerable(), sut);
            Assert.Same(fixtures.AsEnumerable(), fixtures);
        }

        [Fact]
        public void to_array()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToArray();

            Assert.Equal(3, result.Length);
            Assert.Equal(fixtures, result);
        }

        [Fact]
        public void to_list()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToList();

            Assert.Equal(3, result.Count);
            Assert.Equal(fixtures, result);
        }

        [Fact]
        public void to_dictionary()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToDictionary(x => x.Id);

            var expected = fixtures.ToDictionary(x => x.Id);

            Assert.Equal(expected.Keys, result.Keys);
            Assert.Equal(expected.Values, result.Values);
        }

        [Fact]
        public void to_dictionary_with_selected_elements()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToDictionary(x => x.Id, x => x.Value);

            var expected = fixtures.ToDictionary(x => x.Id, x => x.Value);

            Assert.Equal(expected.Keys, result.Keys);
            Assert.Equal(expected.Values.Count, result.Values.Count);

            foreach (var value in expected.Values)
            {
                Assert.Contains(value, result.Values);
            }
        }

        [Fact]
        public void to_lookup()
        {
            var fixtures = CreateSequentialFixtures(3);
            fixtures.Add(new Fixture
            {
                Id = "items/3",
                Value = 0
            });
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToLookup(x => x.Value);

            var expected = fixtures.ToLookup(x => x.Value);

            Assert.Equal(result.Count, expected.Count);

            for (int i = 0; i < result.Count; i++)
            {
                Assert.Equal(result[i].Count(), expected[i].Count());

                for (int j = 0; j < result[i].Count(); j++)
                {
                    Assert.Same(result[i].ToList()[j], expected[i].ToList()[j]);
                }
            }
        }

        [Fact]
        public void to_lookup_with_selected_elements()
        {
            var fixtures = CreateSequentialFixtures(3);
            fixtures.Add(new Fixture
            {
                Id = "items/3",
                Value = 0
            });
            var sut = CreateDynamicList(fixtures);

            var result = sut.ToLookup(x => x.Value, x => x.Id);

            var expected = fixtures.ToLookup(x => x.Value, x => x.Id);

            Assert.Equal(result.Count, expected.Count);

            for (int i = 0; i < result.Count; i++)
            {
                Assert.Equal(result[i].Count(), expected[i].Count());

                for (int j = 0; j < result[i].Count(); j++)
                {
                    Assert.Same(result[i].ToList()[j], expected[i].ToList()[j]);
                }
            }
        }

        [Fact]
        public void of_type()
        {
            var fixtures = new ArrayList();
            fixtures.Add(new RavenJObject());
            fixtures.AddRange(CreateSequentialFixtures(3));
            fixtures.Add(new RavenJObject());

            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.OfType<Fixture>().ToList(), sut.OfType<Fixture>().ToList());
            Assert.Equal(fixtures.OfType<RavenJObject>().ToList().Count, sut.OfType<RavenJObject>().ToList().Count);
        }

        [Fact]
        public void iterate_over_ravenjobjects()
        {
            var dList = new DynamicList(new[]
            {
                new RavenJObject(), new RavenJObject()
            });

            Assert.Equal(2, dList.Count);

            var count = 0;

            foreach (var item in dList)
            {
                count++;
                Assert.IsType<DynamicJsonObject>(item);
            }

            Assert.Equal(2, count);
        }

        [Fact]
        public void iterate_over_ravenjarrayss()
        {
            var dList = new DynamicList(new []
            {
                new RavenJArray(), new RavenJArray()
            });

            Assert.Equal(2, dList.Count);

            var count = 0;

            foreach (var item in dList)
            {
                count++;
                Assert.IsType<DynamicList>(item);
            }

            Assert.Equal(2, count);
        }

        [Fact]
        public void cast_on_objects()
        {
            var fixtures = new List<object>(CreateSequentialFixtures(3));
            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.Cast<Fixture>(), sut.Cast<Fixture>());
        }

        [Fact]
        public void element_at()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Same(fixtures.ElementAt(0), sut.ElementAt(0));
            Assert.Same(fixtures.ElementAt(1), sut.ElementAt(1));
            Assert.Same(fixtures.ElementAt(2), sut.ElementAt(2));
        }

        [Fact]
        public void element_at_or_default_returns_dynamic_null_when_index_is_out_of_range()
        {
            var sut = CreateDynamicList(new List<object>());

            Assert.IsType<DynamicNullObject>(sut.ElementAtOrDefault(100));
        }

        [Fact]
        public void long_count()
        {
            var fixtures = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.LongCount(), sut.LongCount());
        }

        [Fact]
        public void aggregate_items()
        {
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(fixtures);

            var result = sut.Aggregate((current, next) => new Fixture
            {
                Id = current.Id + ";" + next.Id,
                Value = current.Value + next.Value
            });

            var expected = fixtures.Aggregate((current, next) => new Fixture
            {
                Id = current.Id + ";" + next.Id,
                Value = current.Value + next.Value
            });

            Assert.Equal(expected.Value, result.Value);
            Assert.Equal(expected.Id, result.Id);
        }

        [Fact]
        public void aggregate_with_seed()
        {
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(fixtures);

            var result = sut.Aggregate(new Fixture
            {
                Id = "aggregate_of:",
                Value = 0
            }, (current, next) => new Fixture
            {
                Id = current.Id + next.Id + ";",
                Value = current.Value + next.Value
            });

            var expected = fixtures.Aggregate(new Fixture
            {
                Id = "aggregate_of:",
                Value = 0
            }, (current, next) => new Fixture
            {
                Id = current.Id + next.Id + ";",
                Value = current.Value + next.Value
            });

            Assert.Equal(expected.Value, result.Value);
            Assert.Equal(expected.Id, result.Id);
        }

        [Fact]
        public void aggregate_with_seed_and_result_selector()
        {
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(fixtures);

            var result = sut.Aggregate(new Fixture
            {
                Id = "aggregate_of:",
                Value = 0
            }, (current, next) => new Fixture
            {
                Id = current.Id + next.Id + ";",
                Value = current.Value + next.Value
            },
            x => x.Id);

            var expected = fixtures.Aggregate(new Fixture
            {
                Id = "aggregate_of:",
                Value = 0
            }, (current, next) => new Fixture
            {
                Id = current.Id + next.Id + ";",
                Value = current.Value + next.Value
            }, 
            x => x.Id);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void take_while()
        {
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.TakeWhile(x => x.Value < 2), sut.TakeWhile(x => x.Value < 2));
            Assert.Equal(fixtures.TakeWhile((x, i) => i == 3), sut.TakeWhile((x, i) => i == 3));
        }

        [Fact]
        public void skip_while()
        {
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(fixtures);

            Assert.Equal(fixtures.SkipWhile(x => x.Value < 2), sut.SkipWhile(x => x.Value < 2));
            Assert.Equal(fixtures.SkipWhile((x, i) => i < 2), sut.SkipWhile((x, i) => i < 2));
        }

        [Fact]
        public void join_lists()
        {
            var values = new[] { 0, 2 };
            var fixtures = CreateSequentialFixtures(3);
            
            var sut = CreateDynamicList(values);

            var expected = values.Join(fixtures, x => x, x => x.Value, (x, i) => new
            {
                JoinValue = x,
                Identifier = i.Id
            }).ToList();

            var result = sut.Join(fixtures, x => x, x => x.Value, (x, i) => new
            {
                JoinValue = x,
                Identifier = i.Id
            }).ToList();

            Assert.Equal(expected.Count(), result.Count());

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i].Identifier, result[i].Identifier);
                Assert.Equal(expected[i].JoinValue, result[i].JoinValue);
            }
        }

        [Fact]
        public void group_join()
        {
            var values = new[] { 0, 2 };
            var fixtures = CreateSequentialFixtures(3);

            var sut = CreateDynamicList(values);

            var expected = values.GroupJoin(fixtures, x => x, x => x.Value, (x, i) => new
            {
                JoinValue = x,
                Fixtures = i.Select(f => f.Id)
            }).ToList();

            var result = sut.GroupJoin(fixtures, x => x, x => x.Value, (x, i) => new
            {
                JoinValue = x,
                Fixtures = i.Select(f => f.Id)
            }).ToList();

            Assert.Equal(expected.Count(), result.Count());

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i].Fixtures, result[i].Fixtures);
                Assert.Equal(expected[i].JoinValue, result[i].JoinValue);
            }
        }

        [Fact]
        public void concat_can_get_items_of_different_type()
        {
            var fixtures = CreateSequentialFixtures(1);

            var sut = CreateDynamicList(fixtures);

            var result = sut.Concat(new []
            {
                1
            }).ToList();

            Assert.Equal(2, result.Count);
            Assert.IsType<int>(result[1]);

            result = sut.Concat(new dynamic[]
            {
                "2", new object()
            }).ToList();

            Assert.Equal(3, result.Count);
            Assert.IsType<string>(result[1]);
            Assert.IsType<object>(result[2]);
        }

        [Fact]
        public void zip_items()
        {
            var fixtures = CreateSequentialFixtures(3);
            var values = new[] { 99, 100 };
            var sut = CreateDynamicList(fixtures);

            var expected = fixtures.Zip(values, (f, v) => f.Id + " " + v);
            var result = sut.Zip(values, (f, v) => f.Id + " " + v);

            Assert.Equal(expected, result);
        }

        [Fact]
        public void union_items()
        {
            var fixtures1 = CreateSequentialFixtures(3);
            var fixtures2 = CreateSequentialFixtures(3);
            var sut = CreateDynamicList(fixtures1);

            Assert.Equal(fixtures1.Union(fixtures2), sut.Union(fixtures2));

            Assert.Equal(5, sut.Union(new []{1, 3.0}).Count());
        }

        [Fact]
        public void intersect_lists()
        {
            var fixtures1 = CreateSequentialFixtures(3);
            var fixtures2 = new[]
            {
                fixtures1[1]
            };

            var sut = CreateDynamicList(fixtures1);

            Assert.Equal(fixtures1.Intersect(fixtures2), sut.Intersect(fixtures2));
        }

        [Fact]
        public void predicates_and_selectors_works_with_raven_j_objects_inside()
        {
            var sut = CreateDynamicList(new[]
            {
                new RavenJObject
                {
                    { "Id", 1},
                    { "Age", 21 }
                },
                new RavenJObject
                {
                    { "Id", 2},
                    { "Age", 32 }
                },
                new RavenJObject
                {
                    { "Id", 3},
                    { "Age", 24 }
                }
            });

            Assert.Equal(3, sut.ToDictionary(x => x.Id, x => x.Age).Count);
            Assert.Equal(3, sut.ToLookup(x => x.Id, x => x.Age).Count);

            Assert.Equal("1;2;3;", sut.Aggregate(new {Id = 0, Value = 0}, (current, next) => new
            {
                Id = current.Id + next.Id + ";"
            }, x => x.Id));

            Assert.Equal(1, sut.TakeWhile(x => x.Age < 25).Count());
            Assert.Equal(2, sut.SkipWhile(x => x.Age < 25).Count());

            Assert.Equal(1, sut.Join(new []{new {Id = 1}}, x => x.Id, x => x.Id, (x, y) => x.Id + y.Id).Count());
            Assert.Equal(3, sut.GroupJoin(new[] { new { Id = 1 } }, x => x.Id, x => x.Id, (x, y) => "item" + x + " " + y).Count());
            Assert.Equal(1, sut.Zip(new[] { new { Id = 1 } }, (f, v) => f.Id + " " + v).Count());
        }

        private static DynamicList CreateDynamicList(IEnumerable items)
        {
            return new DynamicList(items);
        }

        private static List<Fixture> CreateSequentialFixtures(int numberOfRequestedItems)
        {
            var list = new List<Fixture>();

            for (int i = 0; i < numberOfRequestedItems; i++)
            {
                list.Add(new Fixture
                {
                    Id = "items/" + i,
                    Value = i
                });
            }

            return list;
        }

        public class Fixture : IComparable
        {
            public string Id;
            public int Value;

            public int CompareTo(object obj)
            {
                var fixture = obj as Fixture;

                if (fixture != null)
                    return Value.CompareTo(fixture.Value);

                return -1;
            }
        }
    }
}
