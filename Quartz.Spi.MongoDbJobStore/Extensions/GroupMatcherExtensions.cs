using System;
using MongoDB.Bson;
using Quartz.Impl.Matchers;
using Quartz.Util;

namespace Quartz.Spi.MongoDbJobStore.Extensions
{
    internal static class GroupMatcherExtensions
    {
        public static BsonRegularExpression ToBsonRegularExpression<T>(this GroupMatcher<T> matcher) where T : Key<T>
        {
            if (StringOperator.Equality.Equals(matcher.CompareWithOperator))
            {
                return new BsonRegularExpression($"^{matcher.CompareToValue}$");
            }
            if (StringOperator.Contains.Equals(matcher.CompareWithOperator))
            {
                return new BsonRegularExpression($"{matcher.CompareToValue}");
            }
            if (StringOperator.EndsWith.Equals(matcher.CompareWithOperator))
            {
                return new BsonRegularExpression($"{matcher.CompareToValue}$");
            }
            if (StringOperator.StartsWith.Equals(matcher.CompareWithOperator))
            {
                return new BsonRegularExpression($"^{matcher.CompareToValue}");
            }
            if (StringOperator.Anything.Equals(matcher.CompareWithOperator))
            {
                return new BsonRegularExpression(".*");
            }
            throw new ArgumentOutOfRangeException("Don't know how to translate " + matcher.CompareWithOperator +
                                                  " into BSON regular expression");
        }
    }
}