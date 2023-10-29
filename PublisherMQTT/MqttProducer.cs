using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryPublisher;
using Definitions;
using NLog;
using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using MQTTnet.Extensions.ManagedClient;

namespace PublisherMQTT
{
    public class MqttProducer : IPublisher
    {
        private IConfigurator _configurator;
        private string _id;
        private Logger _logger;

        private string sProtocolVersion = "3.1.1";
        private string sHost;
        private string sPort;
        private string sLevelQoS = "0";

        IManagedMqttClient mqttClient = new MqttFactory().CreateManagedMqttClient();

        public void init(string id, IConfigurator configurator, IGenericLogger logger)
        {
            try
            {
                _configurator = configurator;
                _id = id;
                _logger = (Logger)logger.init("PublisherMQTT");

                object value;
                Dictionary<string, object> dirpublisherConfig = _configurator.getMap(Config.Publishers, _id);
                if (!dirpublisherConfig.TryGetValue(Config.Host, out value))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Host}' en la configuracion del publicador con id '{_id}'");
                }
                sHost = value.ToString();

                if (!dirpublisherConfig.TryGetValue(Config.Port, out value))
                {
                    _logger.Error($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                    throw new Exception($"No se encontro el parametro '{Config.Port}' en la configuracion del publicador con id '{_id}'");
                }
                sPort = value.ToString();

                if (_configurator.getMap(Config.Subscriptors, _id).TryGetValue("version", out value))
                {
                    sProtocolVersion = value.ToString();
                }

                if (_configurator.getMap(Config.Subscriptors, _id).TryGetValue("levelQoS", out value))
                {
                    sLevelQoS = value.ToString();
                }

                // Create client options object
                MqttClientOptionsBuilder builder;
;
                if (sProtocolVersion == "5.0")  //MQTTv5.0
                {
                    builder = new MqttClientOptionsBuilder()
                         .WithClientId(Guid.NewGuid().ToString())
                         .WithTcpServer(sHost, Convert.ToInt32(sPort))
                         .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500);
                }
                else if (sProtocolVersion == "3.1.0")   //MQTTv3.1.0
                {
                    builder = new MqttClientOptionsBuilder()
                        .WithClientId(Guid.NewGuid().ToString())
                        .WithTcpServer(sHost, Convert.ToInt32(sPort))
                        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V310)
                        .WithCleanSession();
                }
                else    //MQTTv3.1.1
                {
                    builder = new MqttClientOptionsBuilder()
                        .WithClientId(Guid.NewGuid().ToString())
                        .WithTcpServer(sHost, Convert.ToInt32(sPort))
                        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V311)
                        .WithCleanSession();
                }

                ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                       .WithAutoReconnectDelay(TimeSpan.FromSeconds(5))         //Autoreconectar con el servidor ante fallas a los 5 segundos
                       .WithClientOptions(builder.Build())
                       .Build();


                var connectResult = mqttClient.StartAsync(options);
                if (!connectResult.Wait(TimeSpan.FromSeconds(30)))   //30 segundos para conectarse al servidor
                {
                    _logger.Error($"30 segundos de tiempo de espera para conexion con el broker 'mqtt://{sHost}:{sPort}/' agotados");
                    throw new Exception($"30 segundos de tiempo de espera para conexion con el broker 'mqtt://{sHost}:{sPort}/' agotados");
                }

            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool publish(string recipient, object payload, string priority = "NORMAL")
        {
            try
            {
                _logger.Trace("Inicio");
                if (payload is byte[])
                {
                    var message = new MqttApplicationMessageBuilder()
                        .WithTopic(recipient)
                        .WithPayload(Encoding.UTF8.GetString((byte[])payload))
                        .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                        .WithRetainFlag()
                        .Build();

                    var toSuscribe = mqttClient.EnqueueAsync(message);
                    if (!toSuscribe.Wait(TimeSpan.FromSeconds(30)))   //30 segundos para publicar al topico en el broker
                    {
                        throw new Exception($"30 segundos de tiempo de espera para publicar al 'topico{recipient}' en el broker 'mqtt://{sHost}:{sPort}/' agotados");
                    }
                    
                    _logger.Trace("Fin");
                    return true;
                }
                else
                {
                    _logger.Error("No está implementada la publicación para el tipo de dato suministrado");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}