using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Definitions;
using InterfaceLibraryConfigurator;
using InterfaceLibraryPublisher;
using System;
using System.Collections.Generic;
using System.Threading;
using InterfaceLibraryLogger;
using NLog;
using System.Text;
using Avro.Generic;
using Newtonsoft.Json;

namespace PublisherKafka
{
    public class KafkaProducer : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;

        private string bootstrapServers;
        private string schemaRegistryUrls;
        private string schemaRegistryType;

        private IProducer<string,string> producerForStr;
        private IProducer<string, GenericRecord> producerForAvro;
        private ProducerConfig prodConfig;
        bool schemaRegistryEnabled = false;


        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _id = id;
                _logger = (Logger)logger.init("PublisherKafka");
                _logger.Trace("Inicio");

                _configurator = configurator;

                object value;
                Dictionary<string, object> dirpublisherConfig = _configurator.getMap(Definitions.Config.Publishers, _id);

                if (!dirpublisherConfig.TryGetValue("bootstrapServers", out value))
                {
                    _logger.Error($"No se encontro el parametro 'bootstrapServers' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro 'bootstrapServers' en la configuracion del publicador con id '{_id}'");
                }
                bootstrapServers = value.ToString();

                if (dirpublisherConfig.ContainsKey("schemaRegistry"))
                {
                    schemaRegistryEnabled = true;
                    if (!dirpublisherConfig.TryGetValue("schemaRegistry", out value))
                    {
                        _logger.Error($"No se encontro el parametro 'schemaRegistry' en la configuracion del publicador con id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'schemaRegistry' en la configuracion del publicador con id '{_id}'");
                    }
                    var dirSchemaRegistry = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                    if (!dirSchemaRegistry.TryGetValue("schemaRegistryUrls", out value))
                    {
                        _logger.Error($"No se encontro el parametro 'schemaRegistryUrls' en la configuracion del publicador con id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'schemaRegistryUrls' en la configuracion del publicador con id '{_id}'");
                    }
                    schemaRegistryUrls = value.ToString();

                    if (!dirSchemaRegistry.TryGetValue("schemaRegistryType", out value))
                    {
                        _logger.Error($"No se encontro el parametro 'schemaRegistryType' en la configuracion del publicador con id '{_id}'");
                        throw new Exception($"No se encontro el parametro 'schemaRegistryType' en la configuracion del publicador con id '{_id}'");
                    }
                    schemaRegistryType = value.ToString();
                    _logger.Debug("Productor para Kafka usa esquema(s) tipo -> '{0}' definido(s) en -> '{1}'", schemaRegistryType, schemaRegistryUrls);
                }

                _logger.Debug("Productor para Kafka en los cluster -> '{0}'", bootstrapServers);



                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool publish(string recipient, object payload, string priority = "NORMAL")
        {
            _logger.Trace("Inicio");
            try
            {
                prodConfig = new ProducerConfig { BootstrapServers = bootstrapServers };

                if (schemaRegistryEnabled)
                {
                    var schemaRegistryConfig = new SchemaRegistryConfig { Url = schemaRegistryUrls };
                    var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

                    if (schemaRegistryType == "avro")
                    {
                        var AvroConfig = new AvroSerializerConfig
                        {
                            SubjectNameStrategy = SubjectNameStrategy.Record,
                        };

                        producerForAvro = new ProducerBuilder<string, GenericRecord>(prodConfig)
                            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, AvroConfig))
                            .Build();
                    }
                    else if (schemaRegistryType == "json") //por hacer pruebas
                    {
                        producerForStr = new ProducerBuilder<string, string>(prodConfig)
                            .SetValueSerializer(new JsonSerializer<string>(schemaRegistry))
                            .Build();
                    }
                }
                else
                {
                    producerForStr = new ProducerBuilder<string, string>(prodConfig)
                        .Build();
                }

                if (payload is byte[])
                {
                    var deliveryReport = producerForStr.ProduceAsync(recipient, new Message<string, string> { Key = " ", Value = Encoding.UTF8.GetString((byte[])payload) });
                    deliveryReport.Wait();
                    DeliveryResult<string, string> deliveryReportR = deliveryReport.Result;

                    if (deliveryReportR.Status == PersistenceStatus.NotPersisted)
                    {
                        _logger.Debug("Mensaje no persistido por motivo desconocido:  Recipient '{0}'  Payload '{1}' ", recipient, Encoding.UTF8.GetString((byte[])payload));
                        _logger.Trace("Fin");
                        return false;
                    }
                    _logger.Debug("Recipient '{0}'  Payload '{1}' ", recipient, Encoding.UTF8.GetString((byte[])payload));
                    _logger.Trace("Fin");
                    return true;
                }
                else if(payload is GenericRecord)
                {
                    var deliveryReport = producerForAvro.ProduceAsync(recipient, new Message<string, GenericRecord> { Key = " ", Value = (GenericRecord)payload });
                    deliveryReport.Wait();
                    DeliveryResult<string, GenericRecord> deliveryReportR = deliveryReport.Result;

                    GenericRecord objTolog = (GenericRecord)payload;
                    if (deliveryReportR.Status == PersistenceStatus.NotPersisted)
                    {
                        _logger.Debug("Mensaje no persistido por motivo desconocido:  Recipient '{0}'  Payload '{1}' ", recipient, objTolog.Schema.ToString());
                        _logger.Trace("Fin");
                        return false;
                    }

                    _logger.Debug("Recipient '{0}'  Payload '{1}' ", recipient, objTolog.ToString());
                    _logger.Debug("Recipient '{0}'  Payload '{1}' ", recipient, objTolog.Schema.ToString());

                    _logger.Trace("Fin");
                    return true;
                }
                else
                {
                    _logger.Error("No está implementada la publicación para el tipo de dato suministrado");
                    return false;
                }
            }
            catch (ProduceException<string, string> e)
            {
                _logger.Error($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                throw e.InnerException;
            }
            catch (ProduceException<string, GenericRecord> e)
            {
                _logger.Error($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                throw e.InnerException;
            }

            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}