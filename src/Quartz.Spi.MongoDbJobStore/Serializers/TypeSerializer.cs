using System;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.Spi.MongoDbJobStore.Serializers
{
    internal class TypeSerializer : SerializerBase<Type>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Type value)
        {
            context.Writer.WriteString($"{value.FullName}, {value.Assembly.GetName().Name}");
        }

        public override Type Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var value = context.Reader.ReadString();
            return Type.GetType(value);
        }
    }
}