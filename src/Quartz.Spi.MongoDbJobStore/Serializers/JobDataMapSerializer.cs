using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Quartz.Simpl;

namespace Quartz.Spi.MongoDbJobStore.Serializers
{
    internal class JobDataMapSerializer : SerializerBase<JobDataMap>
    {
        private readonly DefaultObjectSerializer _objectSerializer = new DefaultObjectSerializer();
        private readonly JsonSerializerSettings _serializerSettings;

        public JobDataMapSerializer()
        {
            _serializerSettings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                TypeNameHandling = TypeNameHandling.Auto,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple
            };
        }

        
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDataMap value)
        {
            if (value.Count == 0)
            {
                context.Writer.WriteNull();
                return;
            }
            
            var json = JsonConvert.SerializeObject(value, _serializerSettings);
            var document = BsonDocument.Parse(json);
            
            var serializer = BsonSerializer.LookupSerializer(typeof(BsonDocument));
            serializer.Serialize(context, document);
        }
        
        public override JobDataMap Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            if (context.Reader.CurrentBsonType == BsonType.Null)
            {
                context.Reader.ReadNull();
                return new JobDataMap();
            }
            
            var serializer = BsonSerializer.LookupSerializer<BsonDocument>();
            var document = serializer.Deserialize(context);
            var json = BsonExtensionMethods.ToJson(document);
            var result = JsonConvert.DeserializeObject<JobDataMap>(json, _serializerSettings)!;
            return result;
        }
    }
}