using System.Collections.Generic;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.Spi.MongoDbJobStore.Serializers
{
    internal class SetSerializer<T> : SerializerBase<ISet<T>>
    {
        private readonly IBsonSerializer _serializer;

        public SetSerializer()
        {
            _serializer = BsonSerializer.LookupSerializer(typeof (IEnumerable<T>));
        }

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, ISet<T> value)
        {
            _serializer.Serialize(context, args, value);
        }

        public override ISet<T> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var enumerable = (IEnumerable<T>)_serializer.Deserialize(context, args);
            return new HashSet<T>(enumerable);
        }
    }
}
