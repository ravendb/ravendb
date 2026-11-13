using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public partial class DynamicArray
    {
        [CanBeNull]
        private static IComparer<object> AsObjectComparer([CanBeNull] IComparer comparer)
        {
            if (comparer == null)
                return null;

            if (ReferenceEquals(comparer, Comparer<char>.Default))
                return new DefaultCharacterComparerAdapter();

            if (comparer is IComparer<char>)
                return new CharacterComparerAdapter(comparer);

            return new NonGenericComparerAdapter(comparer);
        }

        private sealed class DefaultCharacterComparerAdapter : IComparer<object>
        {
            public int Compare(object x, object y)
            {
                var leftIsText = x is LazyStringValue or LazyCompressedStringValue;
                var leftText = leftIsText ? x.ToString() : null;
                var leftIsCharacter = leftIsText && leftText.Length == 1;
                var leftCharacter = leftIsCharacter ? leftText[0] : default;

                var rightIsText = y is LazyStringValue or LazyCompressedStringValue;
                var rightText = rightIsText ? y.ToString() : null;
                var rightIsCharacter = rightIsText && rightText.Length == 1;
                var rightCharacter = rightIsCharacter ? rightText[0] : default;

                if ((leftIsCharacter || x is char) && (rightIsCharacter || y is char))
                    return Comparer<char>.Default.Compare(leftIsCharacter ? leftCharacter : (char)x,
                        rightIsCharacter ? rightCharacter : (char)y);

                object left;
                if (leftIsText)
                    left = leftIsCharacter ? leftCharacter : leftText;
                else
                    left = x is DynamicNullObject ? null : x;

                object right;
                if (rightIsText)
                    right = rightIsCharacter ? rightCharacter : rightText;
                else
                    right = y is DynamicNullObject ? null : y;

                return ((IComparer)Comparer<char>.Default).Compare(left, right);
            }
        }

        private sealed class CharacterComparerAdapter : IComparer<object>
        {
            private readonly IComparer _comparer;

            public CharacterComparerAdapter(IComparer comparer)
            {
                _comparer = comparer;
            }

            public int Compare(object x, object y)
            {
                return _comparer.Compare(ConvertComparerValue(x, isCompareCharacters: true), ConvertComparerValue(y, isCompareCharacters: true));
            }
        }

        private sealed class NonGenericComparerAdapter : IComparer<object>
        {
            private readonly IComparer _comparer;

            public NonGenericComparerAdapter(IComparer comparer)
            {
                _comparer = comparer;
            }

            public int Compare(object x, object y)
            {
                return _comparer.Compare(ConvertComparerValue(x), ConvertComparerValue(y));
            }
        }

        internal static object ConvertComparerValue(object value, bool isCompareCharacters = false)
        {
            if (value is LazyStringValue or LazyCompressedStringValue)
            {
                var text = value.ToString();
                return isCompareCharacters && text.Length == 1
                    ? text[0]
                    : text;
            }

            return value switch
            {
                DynamicNullObject => null,
                _ => value
            };
        }
    }
}
