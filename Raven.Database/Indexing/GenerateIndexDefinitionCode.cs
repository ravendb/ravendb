// -----------------------------------------------------------------------
//  <copyright file="GenerateIndexDefinitionCode.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using ICSharpCode.NRefactory.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.MEF;
using Raven.Client.Indexes;
using Raven.Database.Linq;
using Raven.Database.Plugins;

namespace Raven.Database.Indexing
{
    //TODO: test me!
    public class IndexDefinitionCodeGenerator
    {
        private readonly IndexDefinition _indexDefinition;

        public IndexDefinitionCodeGenerator(IndexDefinition indexDefinition)
        {
            _indexDefinition = indexDefinition;
        }

        public string Generate()
        {
            var indexDeclaration = new TypeDeclaration
            {
                Name = Regex.Replace(_indexDefinition.Name, @"[^\w\d]", ""),
                BaseTypes =
                {
                    new SimpleType("AbstractIndexCreationTask")
                },
                Modifiers = Modifiers.Public,
                Members =
                {
                    new PropertyDeclaration
                    {
                        Name = "IndexName",
                        ReturnType = new PrimitiveType("string"),
                        Modifiers = Modifiers.Public | Modifiers.Override, Getter = new Accessor
                        {
                            Body = new BlockStatement()
                            {
                                new ReturnStatement(new PrimitiveExpression(_indexDefinition.Name))
                            }
                        }
                    }
                }
            };

            var objectCreateExpression = new ObjectCreateExpression(new SimpleType("IndexDefinition"))
            {
                Initializer = new ArrayInitializerExpression()
            };

            if (_indexDefinition.Maps.Count == 1)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Map", new VerbatimStringLiteralExpression(_indexDefinition.Map)));
            }
            else
            {
                var maps = new ArrayInitializerExpression();
                _indexDefinition.Maps.ForEach(map => maps.Elements.Add(new VerbatimStringLiteralExpression(map)));

                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Maps", maps));
            }

            if (_indexDefinition.Reduce != null)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Reduce", new VerbatimStringLiteralExpression(_indexDefinition.Reduce)));
            }

            if (_indexDefinition.MaxIndexOutputsPerDocument != null)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("MaxIndexOutputsPerDocument", new PrimitiveExpression(_indexDefinition.MaxIndexOutputsPerDocument)));
            }

            if (_indexDefinition.Indexes.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Indexes", CreateExpressionFromStringToEnumDictionary(_indexDefinition.Indexes)));
            }

            if (_indexDefinition.Stores.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Stores", CreateExpressionFromStringToEnumDictionary(_indexDefinition.Stores)));
            }

            if (_indexDefinition.TermVectors.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("TermVectors", CreateExpressionFromStringToEnumDictionary(_indexDefinition.TermVectors)));
            }

            if (_indexDefinition.SortOptions.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("SortOptions", CreateExpressionFromStringToEnumDictionary(_indexDefinition.SortOptions)));
            }

            if (_indexDefinition.Analyzers.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("Analyzers", CreateAnalizersExpression(_indexDefinition)));
            }

            if (_indexDefinition.SuggestionsOptions.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("SuggestionsOptions", CreateSuggestionsExpression(_indexDefinition)));
            }

            if (_indexDefinition.SpatialIndexes.Count > 0)
            {
                objectCreateExpression.Initializer.Elements.Add(new NamedExpression("SpatialIndexes", CreateSpatialIndexesExpression(_indexDefinition)));
            }

            var createIndexDefinition = new MethodDeclaration
            {
                Name = "CreateIndexDefinition",
                Modifiers = Modifiers.Public | Modifiers.Override,
                ReturnType = new SimpleType("IndexDefinition"),
                Body = new BlockStatement
                {
                    new ReturnStatement(objectCreateExpression)
                }
            };
            indexDeclaration.Members.Add(createIndexDefinition);

            var namespaces = new HashSet<string>
                {
                    typeof (SystemTime).Namespace,
                    typeof (Enumerable).Namespace,
                    typeof (IEnumerable<>).Namespace,
                    typeof (IEnumerable).Namespace,
                    typeof (int).Namespace,
                    typeof (CultureInfo).Namespace,
                    typeof (Regex).Namespace,
                    typeof (AbstractIndexCreationTask).Namespace,
                    typeof (IndexDefinition).Namespace,
                    typeof (StringDistanceTypes).Namespace,
                };

            var text = QueryParsingUtils.GenerateText(indexDeclaration, new OrderedPartCollection<AbstractDynamicCompilationExtension>(), namespaces);
            return text;
        }

        private static ArrayInitializerExpression CreateExpressionFromStringToEnumDictionary<T>(IEnumerable<KeyValuePair<string, T>> dictionary)
        {
            var elements = new ArrayInitializerExpression();
            dictionary.ForEach(keyValuePair =>
            {
                var property = new ArrayInitializerExpression();
                property.Elements.Add(new StringLiteralExpression(keyValuePair.Key));

                var value = keyValuePair.Value;
                property.Elements.Add(new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType(value.GetType().Name)), value.ToString()));

                elements.Elements.Add(property);
            });
            return elements;
        }

        private static ArrayInitializerExpression CreateAnalizersExpression(IndexDefinition indexDefinition)
        {
            var analyzers = new ArrayInitializerExpression();

            indexDefinition.Analyzers.ForEach(analyzer =>
            {
                var property = new ArrayInitializerExpression();
                property.Elements.Add(new StringLiteralExpression(analyzer.Key));
                property.Elements.Add(new StringLiteralExpression(analyzer.Value));
                analyzers.Elements.Add(property);
            });

            return analyzers;
        }

        private static ArrayInitializerExpression CreateSuggestionsExpression(IndexDefinition indexDefinition)
        {
            var suggestions = new ArrayInitializerExpression();

            indexDefinition.SuggestionsOptions.ForEach(suggestion =>
            {
                suggestions.Elements.Add(new StringLiteralExpression(suggestion));
            });

            return suggestions;
        }

        private static ArrayInitializerExpression CreateSpatialIndexesExpression(IndexDefinition indexDefinition)
        {
            var spatialIndexes = new ArrayInitializerExpression();

            indexDefinition.SpatialIndexes.ForEach(spatialIndex =>
            {
                var property = new ArrayInitializerExpression();
                property.Elements.Add(new StringLiteralExpression(spatialIndex.Key));

                var value = spatialIndex.Value;
                property.Elements.Add(new ObjectCreateExpression
                {
                    Type = new PrimitiveType("SpatialOptions"),
                    Initializer = new ArrayInitializerExpression(
                        new NamedExpression("Type", new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("SpatialFieldType")), value.Type.ToString())),
                        new NamedExpression("Strategy", new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("SpatialSearchStrategy")), value.Strategy.ToString())),
                        new NamedExpression("MaxTreeLevel", new PrimitiveExpression(value.MaxTreeLevel)),
                        new NamedExpression("MinX", new PrimitiveExpression(value.MinX)),
                        new NamedExpression("MaxX", new PrimitiveExpression(value.MaxX)),
                        new NamedExpression("MinY", new PrimitiveExpression(value.MinY)),
                        new NamedExpression("MaxY", new PrimitiveExpression(value.MaxY)),
                        new NamedExpression("Units", new MemberReferenceExpression(new TypeReferenceExpression(new PrimitiveType("SpatialUnits")), value.Units.ToString()))
                    )
                });

                spatialIndexes.Elements.Add(property);
            });

            return spatialIndexes;
        }

    }
}
